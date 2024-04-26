import csv
import sqlite3


class DatabaseConnector:
    """Manages a connection to a SQLite database and populates it with data."""

    def __init__(self, database_file):
        self.connection = sqlite3.connect(database_file)
        self.cursor = self.connection.cursor()

    def populate(self, spreadsheet_folder):
        """Populates the database with data from multiple CSV files."""
        with self.connection:  # Context manager for automatic commit
            self._populate_spreadsheets(
                f"{spreadsheet_folder}/shipping_data_{i}.csv" for i in range(3)
            )

    def _populate_spreadsheets(self, spreadsheet_paths):
        """Processes data from multiple CSV files."""
        product_names = set()
        shipment_data = {}
        for path in spreadsheet_paths:
            with open(path, "r") as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if reader.line_num == 1:
                        continue  # Skip header row
                    if path.endswith("shipping_data_0.csv"):
                        self._process_shipment_0(row, product_names)
                    else:
                        self._process_shipment_data(row, shipment_data)

        for product_name in product_names:
            self.insert_product_if_it_does_not_already_exist(product_name)
        for shipment_id, data in shipment_data.items():
            self.insert_shipment(shipment_id, data)

    def _process_shipment_0(self, row, product_names):
        """Extracts data from the first spreadsheet and updates product names."""
        product_name = row[2]
        product_names.add(product_name)
        # ... rest of processing logic for shipment 0

    def _process_shipment_data(self, row, shipment_data):
        """Extracts data from the second and third spreadsheets and builds shipment data."""
        if row[0] not in shipment_data:
            shipment_data[row[0]] = {"origin": row[1], "destination": row[2], "products": {}}
        shipment_id = row[0]
        product_name = row[1]
        shipment_data[shipment_id]["products"][product_name] = (
            shipment_data[shipment_id]["products"].get(product_name, 0) + 1
        )
        # ... rest of processing logic for shipment data

    def insert_product_if_it_does_not_already_exist(self, product_name):
        """Inserts a new product into the database (if not already present)."""
        query = "INSERT OR IGNORE INTO product (name) VALUES (?)"
        self.cursor.execute(query, (product_name,))

    def insert_shipment(self, shipment_id, shipment_data):
        """Inserts a new shipment record into the database."""
        origin = shipment_data["origin"]
        destination = shipment_data["destination"]
        products = shipment_data["products"]

        # Insert shipment details (origin, destination)
        query = "INSERT OR IGNORE INTO shipment (id, origin, destination) VALUES (?, ?, ?)"
        self.cursor.execute(query, (shipment_id, origin, destination))

        # Insert product quantities with separate queries (potentially more efficient)
        for product_name, quantity in products.items():
            query = "INSERT INTO shipment_product (shipment_id, product_name, quantity) VALUES (?, ?, ?)"
            self.cursor.execute(query, (shipment_id, product_name, quantity))

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    database_connector = DatabaseConnector("shipment_database.db")
    database_connector.populate("./data")
    database_connector.close()
