import sqlite3
import sys


class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class _Hats:
    def __init__(self, connection):
        self.connection = connection

    def insert(self, hat):
        self.connection.execute("""
            INSERT INTO hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
        """, [hat.id, hat.topping, hat.supplier, hat.quantity])


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class _Suppliers:
    def __init__(self, connection):
        self.connection = connection

    def insert(self, supplier):
        self.connection.execute("""
            INSERT INTO suppliers (id, name) VALUES (?, ?)
        """, [supplier.id, supplier.name])


class Order:
    def __init__(self, id, location, hatId):
        self.id = id
        self.location = location
        self.hatId = hatId


class _Orders:
    def __init__(self, connection):
        self.connection = connection

    def insert(self, order):
        self.connection.execute("""
            INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)
        """, [order.id, order.location, order.hatId])


class Repository:
    def __init__(self):
        self.connection = sqlite3.connect('database.db')
        self.hats = _Hats(self.connection)
        self.suppliers = _Suppliers(self.connection)
        self.orders = _Orders(self.connection)

    def close(self):
        self.connection.commit()
        self.connection.close()

    def create_tables(self):
        self.connection.executescript("""
            CREATE TABLE suppliers (
                id      INT     PRIMARY KEY,
                name    TEXT    NOT NULL
            );
            CREATE TABLE hats (
                id          INT     PRIMARY KEY,
                topping     TEXT    NOT NULL,
                supplier    INT     REFERENCES suppliers(id),
                quantity    INT     NOT NULL
            );      
            CREATE TABLE orders (
                id          INT     PRIMARY KEY,
                location    TEXT    NOT NULL,
                hat         INT     REFERENCES hats(id)
            );
        """)

    def insert_hat(self, hat):
        self.hats.insert(hat)

    def insert_supplier(self, supplier):
        self.suppliers.insert(supplier)

    def insert_order(self, order):
        self.orders.insert(order)

    def is_topping_exist(self, topping):
        c = self.connection.cursor()
        c.execute("""
                   SELECT id FROM hats WHERE topping=(?)
               """, [topping])
        record = c.fetchone()
        # return None if no topping available
        if record is not None:
            return True
        return False

    def return_max_order_id(self):
        c = self.connection.cursor()
        c.execute("""
            SELECT max(id) FROM orders
        """)
        max_id = c.fetchone()[0]
        if max_id is None:
            return 0
        else:
            return int(max_id)

    def find_hat_by_topping(self, topping):
        c = self.connection.cursor()
        c.execute("""
            SELECT * FROM hats WHERE topping=(?)
            ORDER BY supplier ASC
            """, [topping])
        hat = Hat(*c.fetchone())
        return hat

    def find_supplier_name_by_id(self, id):
        c = self.connection.cursor()
        c.execute("""
                   SELECT name FROM suppliers WHERE id=(?)
                   """, [id])
        return c.fetchone()[0]

    def update_hat_quantity(self, hat):
        c = self.connection.cursor()
        if hat.quantity==1:
            c.execute("""
                DELETE FROM hats WHERE id=(?)
                """, [hat.id])
        else:
            c.execute("""
                UPDATE hats SET quantity=(?) WHERE id=(?)
                """, [hat.quantity-1, hat.id])


def update_output_file(output_file, topping, supplier, location):
    output_file.write("{},{},{}\n".format(topping, supplier, location))


def handle_orders(repo):
    input_file = open(sys.argv[2])
    output_file = open(sys.argv[3], 'w')
    order_lines = input_file.read().split('\n')
    for line in order_lines:
        order_words = line.split(",")
        topping = order_words[1]
        location = order_words[0]
        if repo.is_topping_exist(topping):
            # add to orders table
            curr_id = repo.return_max_order_id()+1
            hat = repo.find_hat_by_topping(topping)
            order = Order(curr_id, location, hat.id)
            repo.insert_order(order)

            # update hats table according to quantity (-1), if quantity is 0 then delete record from table
            repo.update_hat_quantity(hat)

            # add to output
            supplier_name = repo.find_supplier_name_by_id(hat.supplier)
            update_output_file(output_file, topping, supplier_name, location)
    output_file.close()
    input_file.close()


def read_conf_file_to_database(repo):
    inputFile = open(sys.argv[1])
    numLine = inputFile.readline().replace('\n', "")
    nums = numLine.split(',')
    nums = [int(n) for n in nums]
    lines = inputFile.read().split('\n')
    hats_lines = lines[:nums[0]]
    suppliers_lines = lines[nums[0]:nums[0] + nums[1]]
    insert_to_DB(hats_lines, repo.insert_hat, Hat)
    insert_to_DB(suppliers_lines, repo.insert_supplier, Supplier)


def insert_to_DB(lines, insert_function, obj_type):
    for line in lines:
        values = line.split(',')
        record = obj_type(*values)
        insert_function(record)


def main():
    repo = Repository()
    repo.create_tables()
    read_conf_file_to_database(repo)
    handle_orders(repo)
    repo.close()


if __name__ == '__main__':
    main()



