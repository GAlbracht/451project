import psycopg2
import json

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="milestone1db",
            user="postgres",
            password="admin",
            host="localhost"
        )
        return conn
    except Exception as e:
        print(f"error: {e}")

def get_states(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT state FROM business ORDER BY state;")
        states = cur.fetchall()
        return states

def get_cities(conn, selected_state):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT city FROM business WHERE state=%s ORDER BY city;", (selected_state,))
        cities = cur.fetchall()
        return cities

def get_businesses(conn, selected_city, selected_state):
    with conn.cursor() as cur:
        cur.execute("SELECT name, city, state FROM business WHERE city=%s AND state=%s ORDER BY name;", (selected_city, selected_state))
        businesses = cur.fetchall()
        return businesses

def get_zipcodes(conn, selected_city, selected_state):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT postal_code FROM businesses WHERE city=%s AND state=%s ORDER BY postal_code;", (selected_city, selected_state))
        zipcodes = cur.fetchall()
        return zipcodes

def get_categories(conn, selected_zipcode):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT UNNEST(string_to_array(categories, ', ')) AS category
            FROM businesses
            WHERE postal_code=%s
            ORDER BY category;
        """, (selected_zipcode,))
        categories = cur.fetchall()
        return categories

def get_businesses_by_category(conn, selected_zipcode, selected_category):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, city, state, stars, review_count, reviewrating, "numCheckins",
            is_open, hours
            FROM businesses
            WHERE postal_code = %s AND categories LIKE %s
            ORDER BY name;
        """, (selected_zipcode, '%' + selected_category + '%',))
        businesses = cur.fetchall()
        return businesses


from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QComboBox, QVBoxLayout, QHBoxLayout,
    QWidget, QListWidget, QTableWidget, QTableWidgetItem, QLabel,
    QPushButton, QGroupBox, QGridLayout
)

class MyApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.conn = connect_db() 
        self.setWindowTitle("Milestone 1")
        self.setGeometry(100, 100, 1000, 500)
        self.initUI()

    def initUI(self):
        mainLayout = QVBoxLayout()

        topRowLayout = QHBoxLayout()
        
        locationGroupBox = QGroupBox("Select Location")
        locationLayout = QGridLayout()
        locationLayout.addWidget(QLabel("State"), 0, 0)
        self.stateComboBox = QComboBox()
        locationLayout.addWidget(self.stateComboBox, 0, 1)
        locationLayout.addWidget(QLabel("City"), 1, 0)
        self.cityListWidget = QListWidget()
        locationLayout.addWidget(self.cityListWidget, 1, 1)
        locationLayout.addWidget(QLabel("Zip Code"), 2, 0)
        self.zipcodeListWidget = QListWidget()
        locationLayout.addWidget(self.zipcodeListWidget, 2, 1)
        locationGroupBox.setLayout(locationLayout)

        statsGroupBox = QGroupBox("Zipcode Statistics")
        statsLayout = QVBoxLayout()
        self.statsTable = QTableWidget()
        self.statsTable.setRowCount(1)
        self.statsTable.setColumnCount(3)
        self.statsTable.setHorizontalHeaderLabels(["# of Businesses", "Total Population", "Average Income"])
        self.statsTable.horizontalHeader().setStretchLastSection(True)
        statsLayout.addWidget(self.statsTable)
        statsGroupBox.setLayout(statsLayout)

        categoriesGroupBox = QGroupBox("Top Categories")
        categoriesLayout = QVBoxLayout()
        self.categoriesTable = QTableWidget()
        self.categoriesTable.setRowCount(1)
        self.categoriesTable.setColumnCount(2)
        self.categoriesTable.setHorizontalHeaderLabels(["Category", "# of Businesses"])
        categoriesLayout.addWidget(self.categoriesTable)
        categoriesGroupBox.setLayout(categoriesLayout)

        self.searchButton = QPushButton('Search')

        topRowLayout.addWidget(locationGroupBox)
        topRowLayout.addWidget(statsGroupBox)
        topRowLayout.addWidget(categoriesGroupBox)
        topRowLayout.addWidget(self.searchButton)
        
        mainLayout.addLayout(topRowLayout)

        secondRowLayout = QHBoxLayout()

        centralWidget = QWidget()
        centralWidget.setLayout(mainLayout)
        self.setCentralWidget(centralWidget)

        filterGroupBox = QGroupBox("Filter on Categories")
        filterLayout = QVBoxLayout()
        self.filterListWidget = QListWidget()

        filterLayout.addWidget(self.filterListWidget)
        filterGroupBox.setLayout(filterLayout)

        self.businessTable = QTableWidget(0, 6)
        self.businessTable.setHorizontalHeaderLabels([
            "Name", "City", "State", "Stars", "Review Count", "Review Rating"
        ])

        secondRowLayout.addWidget(filterGroupBox)
        secondRowLayout.addWidget(self.businessTable)

        mainLayout.addLayout(secondRowLayout)
        
        centralWidget = QWidget()
        centralWidget.setLayout(mainLayout)
        self.setCentralWidget(centralWidget)

        thirdRowLayout = QHBoxLayout()

        popularGroupBox = QGroupBox("Popular Businesses (in zipcode)")
        popularLayout = QVBoxLayout()
        self.popularBusinessTable = QTableWidget(0, 4)
        self.popularBusinessTable.setHorizontalHeaderLabels([
            "Business Name", "Stars", "Review Count", "# of Checkins"
        ])
        popularLayout.addWidget(self.popularBusinessTable)
        popularGroupBox.setLayout(popularLayout)

        successfulGroupBox = QGroupBox("Successful Businesses (in zipcode)")
        successfulLayout = QVBoxLayout()
        self.successfulBusinessTable = QTableWidget(0, 4)
        self.successfulBusinessTable.setHorizontalHeaderLabels([
            "Business Name", "Review Rating", "# of Reviews", "# of Checkins"
        ])
        successfulLayout.addWidget(self.successfulBusinessTable)
        successfulGroupBox.setLayout(successfulLayout)

        thirdRowLayout.addWidget(popularGroupBox)
        thirdRowLayout.addWidget(successfulGroupBox)

        mainLayout.addLayout(thirdRowLayout)
        
        centralWidget = QWidget()
        centralWidget.setLayout(mainLayout)
        self.setCentralWidget(centralWidget)

        self.load_states()

    def load_states(self):
        states = get_states(self.conn)
        for state in states:
            self.stateComboBox.addItem(state[0])
        self.stateComboBox.activated[str].connect(self.on_state_changed) 


    def on_state_changed(self, state):
        self.cityListWidget.clear()
        self.zipcodeListWidget.clear()
        self.categoryListWidget.clear()
        self.businessTable.setRowCount(0)  
        cities = get_cities(self.conn, state)
        for city in cities:
            self.cityListWidget.addItem(city[0])

    def on_city_selected(self):
        selected_items = self.cityListWidget.selectedItems()
        if selected_items:
            selected_city = selected_items[0].text()
            state = self.stateComboBox.currentText()
            zipcodes = get_zipcodes(self.conn, selected_city, state)
            self.zipcodeListWidget.clear()
            self.categoryListWidget.clear()
            self.businessTable.setRowCount(0)  
            for zipcode in zipcodes:
                self.zipcodeListWidget.addItem(zipcode[0])


    def load_businesses(self, city, state):
        self.businessTable.setRowCount(0)
        businesses = get_businesses(self.conn, city, state)
        for business in businesses:
            row_position = self.businessTable.rowCount()
            self.businessTable.insertRow(row_position)
            self.businessTable.setItem(row_position, 0, QTableWidgetItem(business[0]))
            self.businessTable.setItem(row_position, 1, QTableWidgetItem(business[1]))
            self.businessTable.setItem(row_position, 2, QTableWidgetItem(business[2]))

    def on_zipcode_selected(self):
        selected_items = self.zipcodeListWidget.selectedItems()
        if selected_items:
            selected_zipcode = selected_items[0].text()
            categories = get_categories(self.conn, selected_zipcode)
            self.categoryListWidget.clear()
            self.businessTable.setRowCount(0)  
            for category in categories:
                self.categoryListWidget.addItem(category[0])

    def on_category_selected(self):
        selected_zipcode_items = self.zipcodeListWidget.selectedItems()
        selected_category_items = self.categoryListWidget.selectedItems()
        if selected_zipcode_items and selected_category_items:
            selected_zipcode = selected_zipcode_items[0].text()
            selected_category = selected_category_items[0].text()
            self.load_businesses_by_category(selected_zipcode, selected_category)

    def load_businesses_by_category(self, zipcode, category):
        businesses = get_businesses_by_category(self.conn, zipcode, category)
        self.businessTable.setRowCount(0)
        for business in businesses:
            row_position = self.businessTable.rowCount()
            self.businessTable.insertRow(row_position)
            for i, property in enumerate(business):
                if i == 7: 
                    property = "Yes" if property else "No"
                elif i == 8:  
                    property = json.dumps(property) if isinstance(property, dict) else property
                self.businessTable.setItem(row_position, i, QTableWidgetItem(str(property)))

    def on_search_clicked(self):
        state = self.stateComboBox.currentText()
        city_item = self.cityListWidget.currentItem()
        city = city_item.text() if city_item else None
        zipcode_item = self.zipcodeListWidget.currentItem()
        zipcode = zipcode_item.text() if zipcode_item else None
        
        if city and zipcode:
            self.load_businesses(city, state)
            self.update_zipcode_stats(zipcode)
            self.update_popular_businesses(zipcode)
            self.update_successful_businesses(zipcode)
        else:
            # Handle case where city or zipcode is not selected
            pass

    
    def update_zipcode_stats(self, zipcode):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*), AVG(population), AVG(income)
                FROM business JOIN census ON business.postal_code = census.zipcode
                WHERE postal_code = %s;
            """, (zipcode,))
            result = cur.fetchone()
            if result:
                self.zipcodeStatsLabel.setText(f"Total Businesses: {result[0]}\nPopulation: {result[1]}\nAverage Income: {result[2]}")
        
    
    def update_popular_businesses(self, zipcode):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT name, review_count
                FROM businesses
                WHERE postal_code = %s
                ORDER BY review_count DESC
                LIMIT 5;
            """, (zipcode,))
            businesses = cur.fetchall()
            self.popularBusinessListWidget.clear()
            for business in businesses:
                self.popularBusinessListWidget.addItem(f"{business[0]} ({business[1]} reviews)")
        
    
    def update_successful_businesses(self, zipcode):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT name, AVG(stars) as average_stars
                FROM reviews
                JOIN businesses ON businesses.id = reviews.business_id
                WHERE businesses.postal_code = %s
                GROUP BY businesses.name
                ORDER BY average_stars DESC
                LIMIT 5;
            """, (zipcode,))
            businesses = cur.fetchall()
            self.successfulBusinessListWidget.clear()
            for business in businesses:
                self.successfulBusinessListWidget.addItem(f"{business[0]} ({business[1]} average stars)")
    


if __name__ == '__main__':
    app = QApplication([])
    ex = MyApp()
    ex.show()
    app.exec_()