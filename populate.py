import json
import psycopg2
import datetime
import requests

def connect_db():
    try:
        return psycopg2.connect(
            dbname="milestone1db",
            user="postgres",
            password="admin",
            host="localhost"
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def cleanStr4SQL(s):
    return s.replace("'", "''")


def calculate_business_age(registration_date):
    today = datetime.datetime.now()
    age = today.year - registration_date.year - ((today.month, today.day) < (registration_date.month, registration_date.day))
    return age

def calculate_success_score(age, repeat_checkins, positive_reviews, total_checkins, total_reviews):
    age_weight = 0.3
    checkin_weight = 0.4
    review_weight = 0.3

    repeat_checkin_rate = repeat_checkins / total_checkins if total_checkins > 0 else 0
    positive_review_rate = positive_reviews / total_reviews if total_reviews > 0 else 0

    return (age_weight * age) + (checkin_weight * repeat_checkin_rate) + (review_weight * positive_review_rate)


def fetch_data_from_census(api_url):
    response = requests.get(api_url)
    return response.json()[1:] if response.status_code == 200 else []


def fetch_and_process_census_data():
    conn = connect_db()
    if not conn:
        return

    population_url = "https://api.census.gov/data/2020/acs/acs5?get=NAME,B01003_001E&for=zip%20code%20tabulation%20area:*"
    income_url = "https://api.census.gov/data/2020/acs/acs5/subject?get=NAME,S1903_C03_001E&for=zip%20code%20tabulation%20area:*"

    population_data = fetch_data_from_census(population_url)
    income_data = fetch_data_from_census(income_url)

    zip_population = {row[4]: int(row[2]) for row in population_data}
    zip_income = {row[3]: float(row[2]) for row in income_data if row[1] != "-666666666"}

    combined_data = [(zip_code, zip_population.get(zip_code, 0), zip_income.get(zip_code, 0.0)) for zip_code in set(zip_population) | set(zip_income)]
    insert_data_into_db(conn, combined_data)
    conn.close()



def insert_data_into_db(conn, data):
    with conn.cursor() as cursor:
        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS Zipcodes (
        #         zip_code VARCHAR(5) PRIMARY KEY,
        #         population INT,
        #         avg_income NUMERIC(10, 1)
        #     );
        # """)
        psycopg2.extras.execute_values(cursor, """
            INSERT INTO Zipcodes (zip_code, population, avg_income) VALUES %s
            ON CONFLICT (zip_code) DO UPDATE SET
            population = EXCLUDED.population,
            avg_income = EXCLUDED.avg_income;
        """, data)
        conn.commit()

def import_business_data(json_file_path, connection):
    with connection.cursor() as cursor, open(json_file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            registration_date = datetime.datetime.strptime(data.get("registration_date", "2000-01-01"), "%Y-%m-%d")
            business_age = calculate_business_age(registration_date)
            repeat_checkins = data.get("repeat_checkins", 0)
            positive_reviews = data.get("positive_reviews", 0)
            total_checkins = data.get("total_checkins", 1)
            total_reviews = data.get("total_reviews", 1)  

            success_score = calculate_success_score(business_age, repeat_checkins, positive_reviews, total_checkins, total_reviews)

            # Additional business data parsing
            attributes = json.dumps(data.get("attributes", {}))
            hours = json.dumps(data.get("hours", {}))
            is_open = 'TRUE' if data["is_open"] else 'FALSE'
            
            categories = data.get("categories", "")
            if isinstance(categories, list):
                categories = ', '.join(categories)  
            categories = cleanStr4SQL(categories)  

            sql = f"""
            INSERT INTO Businesses (
                business_id, name, neighborhood, address, city, state, postal_code,
                latitude, longitude, stars, review_count, is_open, attributes, categories, hours, numCheckins, reviewrating, business_age, success_score
            ) VALUES (
                '{data["business_id"]}', '{cleanStr4SQL(data["name"])}', '{cleanStr4SQL(data.get("neighborhood", ""))}',
                '{cleanStr4SQL(data["address"])}', '{data["city"]}', '{data["state"]}',
                '{data["postal_code"]}', {data["latitude"]}, {data["longitude"]},
                {data["stars"]}, {data["review_count"]}, {is_open},
                '{attributes}', '{categories}', '{hours}', {total_checkins}, 0.0, {business_age}, {success_score}
            ) ON CONFLICT (business_id) DO UPDATE SET
                business_age = EXCLUDED.business_age,
                success_score = EXCLUDED.success_score;
            """
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f"Failed to insert data: {e}")
                connection.rollback()
        connection.commit()


def import_checkin_data(json_file_path, connection):
    with connection.cursor() as cursor, open(json_file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            business_id = data['business_id']
            for day, times in data['time'].items():
                for hour, count in times.items():
                    sql = f"""
                    INSERT INTO CheckIns (business_id, day, hour, count) VALUES 
                    ('{business_id}', '{day}', '{hour}', {count})
                    ON CONFLICT DO NOTHING;
                    """
                    try:
                        cursor.execute(sql)
                    except Exception as e:
                        print(f"Failed to insert check-in data: {e}")
                        connection.rollback()
        connection.commit()


def import_review_data(json_file_path, connection):
    with connection.cursor() as cursor, open(json_file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            text = cleanStr4SQL(data.get("text", ""))

            sql = f"""
            INSERT INTO Reviews (
                review_id, user_id, business_id, stars, date, text, useful, funny, cool
            ) VALUES (
                '{data["review_id"]}', '{data["user_id"]}', '{data["business_id"]}',
                {data["stars"]}, '{data["date"]}', '{text}',
                {data.get("useful", 0)}, {data.get("funny", 0)}, {data.get("cool", 0)}
            ) ON CONFLICT (review_id) DO NOTHING;
            """
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f"Failed to insert review data: {e}")
                connection.rollback()
        connection.commit()


def import_user_data(json_file_path, connection):
    with connection.cursor() as cursor, open(json_file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            name = cleanStr4SQL(data.get("name", ""))
            friends = '{' + ','.join([cleanStr4SQL(friend) for friend in data.get("friends", [])]) + '}'
            elite = '{' + ','.join(map(str, data.get("elite", []))) + '}'

            sql = f"""
            INSERT INTO Users (
                user_id, name, review_count, average_stars, useful, funny, cool,
                friends, elite, fans, compliment_cool, compliment_cute, compliment_funny,
                compliment_hot, compliment_list, compliment_more, compliment_note, compliment_photos,
                compliment_plain, compliment_profile, compliment_writer, yelping_since
            ) VALUES (
                '{data["user_id"]}', '{name}', {data["review_count"]}, {data["average_stars"]},
                {data.get("useful", 0)}, {data.get("funny", 0)}, {data.get("cool", 0)},
                '{friends}', '{elite}', {data["fans"]}, {data.get("compliment_cool", 0)},
                {data.get("compliment_cute", 0)}, {data.get("compliment_funny", 0)},
                {data.get("compliment_hot", 0)}, {data.get("compliment_list", 0)},
                {data.get("compliment_more", 0)}, {data.get("compliment_note", 0)},
                {data.get("compliment_photos", 0)}, {data.get("compliment_plain", 0)},
                {data.get("compliment_profile", 0)}, {data.get("compliment_writer", 0)},
                '{data["yelping_since"]}'
            ) ON CONFLICT (user_id) DO NOTHING;
            """
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f"Failed to insert user data: {e}")
                connection.rollback()
        connection.commit()


if __name__ == "__main__":
    conn = connect_db()
    if conn:
        fetch_and_process_census_data()
        import_business_data('yelpDB/yelp_business.json', conn)
        import_checkin_data('yelpDB/yelp_checkin.json', conn)
        import_user_data('yelpDB/yelp_user.json', conn)
        import_review_data('yelpDB/yelp_review.json', conn)
        conn.close()
    else:
        print("Failed to connect to the database.")