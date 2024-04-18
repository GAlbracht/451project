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
        self.cityListWidget.setSelectionMode(QListWidget.SingleSelection)  
        self.cityListWidget.itemSelectionChanged.connect(self.on_city_selected)
        locationLayout.addWidget(self.cityListWidget, 1, 1)
        locationLayout.addWidget(QLabel("Zip Code"), 2, 0)
        self.zipcodeListWidget = QListWidget()
        self.zipcodeListWidget.setSelectionMode(QListWidget.SingleSelection)
        self.zipcodeListWidget.itemSelectionChanged.connect(self.on_zipcode_selected)
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
        self.filterListWidget.itemSelectionChanged.connect(self.on_category_selected)

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
        self.popularBusinessTable = QTableWidget(0, 3)  # Three columns now
        self.popularBusinessTable.setHorizontalHeaderLabels([
            "Business Name", "Stars", "Review Count"
        ])
        popularLayout.addWidget(self.popularBusinessTable)
        popularGroupBox.setLayout(popularLayout)

        successfulGroupBox = QGroupBox("Successful Businesses (in zipcode)")
        successfulLayout = QVBoxLayout()
        self.successfulBusinessTable = QTableWidget(0, 2)  # Two columns now
        self.successfulBusinessTable.setHorizontalHeaderLabels([
            "Review Count", "Number of Checkins"
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
        self.filterListWidget.clear()
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
            self.filterListWidget.clear()
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
            
            # Clear previous data
            self.filterListWidget.clear()
            self.statsTable.clearContents()
            self.categoriesTable.clearContents()

            # Load categories for the filterCategory table
            categories = get_categories(self.conn, selected_zipcode)
            for category in categories:
                self.filterListWidget.addItem(category[0])

            # Populate the number of businesses in zipcode statistics table
            self.update_zipcode_stats(selected_zipcode)
            
            # Fill in the top categories table
            self.update_top_categories(selected_zipcode)

    def on_category_selected(self):
        selected_items = self.filterListWidget.selectedItems()
        if selected_items:
            selected_category = selected_items[0].text()
            selected_zipcode_items = self.zipcodeListWidget.selectedItems()
            if selected_zipcode_items:
                selected_zipcode = selected_zipcode_items[0].text()
                self.load_businesses_by_category(selected_zipcode, selected_category)
                self.update_popular_businesses(selected_zipcode, selected_category)
                self.update_successful_businesses(selected_zipcode, selected_category)

    def load_businesses_by_category(self, zipcode, category):
        # Assuming this method already exists and works properly
        # Fetch businesses matching the selected category and zipcode
        businesses = get_businesses_by_category(self.conn, zipcode, category)
        self.businessTable.setRowCount(0)  # Clear existing rows
        for business in businesses:
            row_position = self.businessTable.rowCount()
            self.businessTable.insertRow(row_position)
            # Assuming business has attributes: name, city, state, stars, review_count, review_rating
            for i, property in enumerate(business):
                if i == 7:  # Assuming index 7 is a boolean for "is_open"
                    property = "Yes" if property else "No"
                elif i == 8:  # Assuming index 8 is a dictionary for "hours"
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
        # Query the database for the number of businesses in the zipcode
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM businesses
                WHERE postal_code = %s;
            """, (zipcode,))
            business_count = cur.fetchone()[0]
            
            # Assume the first row is dedicated to the statistics
            self.statsTable.setItem(0, 0, QTableWidgetItem(str(business_count)))

            # ... Include additional code to populate the total population and average income ...

        
    
    def update_popular_businesses(self, zipcode, category):
        with self.conn.cursor() as cur:
            # Modify the query to include category filtering
            cur.execute("""
                SELECT name, stars, review_count
                FROM businesses
                WHERE postal_code = %s AND categories LIKE %s
                ORDER BY review_count DESC
                LIMIT 5;
            """, (zipcode, '%' + category + '%',))
            businesses = cur.fetchall()
            self.popularBusinessTable.setRowCount(0)
            for business in businesses:
                row_position = self.popularBusinessTable.rowCount()
                self.popularBusinessTable.insertRow(row_position)
                for i, value in enumerate(business):
                    self.popularBusinessTable.setItem(row_position, i, QTableWidgetItem(str(value)))

    def update_successful_businesses(self, zipcode, category):
        with self.conn.cursor() as cur:
            # Modify the query to include category filtering
            cur.execute("""
                SELECT review_count, "numCheckins"
                FROM businesses
                WHERE postal_code = %s AND categories LIKE %s
                ORDER BY "numCheckins" DESC
                LIMIT 5;
            """, (zipcode, '%' + category + '%',))
            businesses = cur.fetchall()
            self.successfulBusinessTable.setRowCount(0)
            for business in businesses:
                row_position = self.successfulBusinessTable.rowCount()
                self.successfulBusinessTable.insertRow(row_position)
                for i, value in enumerate(business):
                    self.successfulBusinessTable.setItem(row_position, i, QTableWidgetItem(str(value)))


    
    def update_top_categories(self, zipcode):
        # Assuming categories are stored in a single column separated by commas
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT UNNEST(string_to_array(categories, ', ')) AS category, COUNT(*) 
                FROM businesses 
                WHERE postal_code = %s
                GROUP BY UNNEST(string_to_array(categories, ', '))
                ORDER BY COUNT(*) DESC;
            """, (zipcode,))
            categories = cur.fetchall()
            
            # Populate the categoriesTable with the categories and their business counts
            self.categoriesTable.setRowCount(len(categories))
            for i, (category, count) in enumerate(categories):
                self.categoriesTable.setItem(i, 0, QTableWidgetItem(category))
                self.categoriesTable.setItem(i, 1, QTableWidgetItem(str(count)))


if __name__ == '__main__':
    app = QApplication([])
    ex = MyApp()
    ex.show()
    app.exec_()