import psycopg2
import argparse
import json
import os


data_table = {
    "Person": ['Name', 'Last_name', 'Password', 'Email'],
    "Customer": ["Person_id", "Phone", "Adress_id"],
    "Employee": ["Person_id", "Role", "Restaurant_id"],
    "Delivery_men": ["Employe_id", "Vehicle"],
    "Adress": ['Name', "Adress", "ZipCode", "City"],
    "Restaurant": ['Name', "Description", "Phone", "Picture", "Adress_id"],
    "Purchase": ['State', 'Date', 'Paid_Price', 'Restaurant_id', 'Person_id', 'Adress_id'],
    "Purchase_Aliment": ['Purchase_id', 'Aliment_id'],
    "Aliment": ['Name', 'Price', 'Category', 'Description', 'Picture'],
    "Recipe": ['Aliment_id', 'Ingredient_id', 'Quantity', 'is_public'],
    "Ingredient": ['Name', 'Unit', 'Price_per_unit', 'SoldOut_treshold'],
    "Stock": ['Restaurant_id', 'Ingredient_id', 'Quantity']
}


def insert_data(cur, conn):
    path = os.getcwd()
    with open(os.path.join(path, "data.json")) as file:
        data = json.load(file)

    for table in data:
        cmd = "INSERT INTO public." + table + "("

        for col in data_table[table]:
            cmd += col + ","

        cmd = cmd[:-1] + ") VALUES "

        for wanted_data in data[table]:
            cmd += "("
            for col in data_table[table]:
                try:
                    if type(wanted_data[col]) == str:
                        cmd += "'" + wanted_data[col] + "',"
                    else:
                        cmd += str(wanted_data[col]) + ","
                except KeyError:
                    cmd += "Null,"
            cmd = cmd[:-1] + "),"
        cmd = cmd[:-1] + ';'

        print(cmd)
        cur.execute(cmd)
        conn.commit()


def show_query(queries):
    for query in queries:
        print(query)


def get_purchase():
    cmd = "SELECT Purchase.State, Purchase.date, Person.Name, Person.Last_name, Restaurant.Name, " \
          "Adress.Name, Adress.Adress, Adress.ZipCode, Adress.City FROM Purchase " \
          "INNER JOIN Person ON Purchase.Person_id = Person.id " \
          "INNER JOIN Restaurant ON Purchase.Restaurant_id = Restaurant.id " \
          "INNER JOIN Adress ON Purchase.Adress_id = Adress.id "
    return cmd


def aliment_available(cur, restaurant):
    s = "SELECT id, Name FROM Aliment;"
    aliment = execute_sql_cmd(cur, s)

    s = "SELECT Ingredient_id, Quantity From Stock " \
        "WHERE Restaurant_id = %d;" % restaurant

    for id, name in aliment:
        s = "SELECT Recipe.Ingredient_id, Recipe.Quantity, Stock.Ingredient_id, Stock.Restaurant_id, Stock.Quantity " \
            "FROM Recipe FULL OUTER JOIN Stock " \
            "ON Recipe.Ingredient_id = Stock.Ingredient_id " \
            "WHERE Recipe.Aliment_id = %d AND Recipe.Quantity < Stock.Quantity AND Stock.Restaurant_id = %d;" % (id, restaurant)

        recipe_in_stock = execute_sql_cmd(cur, s)

        s = "SELECT * FROM Recipe " \
            "WHERE Aliment_id = %d;" % id
        recipe_of_aliment = execute_sql_cmd(cur, s)

        if len(recipe_in_stock) == len(recipe_of_aliment):
            print(name)
    return s


def check_command_restaurant(cur, restaurant, state=None):
    cmd = get_purchase()
    cmd += "WHERE Restaurant.id = %d;" % restaurant

    if state:
        cmd = cmd[:-1] + "AND State = '%s';" % state

    show_query(execute_sql_cmd(cur, cmd))


def check_command_customers(cur, customer, state=None):
    cmd = get_purchase()
    cmd += "WHERE Person.id = %d;" % customer

    if state:
        cmd = cmd[:-1] + " AND State = '%s';" % state

    show_query(execute_sql_cmd(cur, cmd))


def show_aliment_order(cur, order):
    cmd = "SELECT Aliment.Name, Aliment.Price From Purchase_Aliment " \
          "INNER JOIN Purchase ON Purchase_Aliment.Purchase_id = Purchase.id " \
          "INNER JOIN Aliment ON Purchase_Aliment.Aliment_id = Aliment.id " \
          "WHERE Purchase_id = %d;" % order

    show_query(execute_sql_cmd(cur, cmd))


def show_recipe(cur, aliment):
    cmd = "SELECT Aliment.Name, Ingredient.Name, Recipe.Quantity From Recipe " \
          "INNER JOIN Ingredient ON Recipe.Ingredient_id = Ingredient.id " \
          "INNER JOIN Aliment ON Recipe.Aliment_id = Aliment.id " \
          "WHERE Aliment_id = %d;" % aliment

    show_query(execute_sql_cmd(cur, cmd))


def show_employee(cur, restaurant):
    cmd = "SELECT Person.Name, Person.Last_name, Role, Restaurant.Name From Employee " \
          "INNER JOIN Person ON Employee.Person_id = Person.id " \
          "INNER JOIN Restaurant ON Employee.Restaurant_id = Restaurant.id " \
          "WHERE Restaurant_id = %d;" % restaurant

    show_query(execute_sql_cmd(cur, cmd))


def execute_sql_cmd(cur, cmd):
    cur.execute(cmd)
    list_tables = cur.fetchall()
    return list_tables


def main():
    HOST = "localhost"
    USER = "nathan"
    PASSWORD = "Amilo123"
    DATABASE = "oc_pizza"

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))

    cur = conn.cursor()

    parser = argparse.ArgumentParser(description='Process with database.')
    parser.add_argument(
        '--insert_data', type=bool, dest='insert_data',
    )
    parser.add_argument(
        '--aliment_available', type=int, dest='aliment_available',
    )
    parser.add_argument(
        '--restaurant_order', type=int, dest='restaurant_order',
    )
    parser.add_argument(
        '--customer_order', type=int, dest='customer_order',
    )
    parser.add_argument(
        '--state', type=str, dest='state',
    )
    parser.add_argument(
        '--aliment_order', type=int, dest='aliment_order',
    )
    parser.add_argument(
        '--recipe', type=int, dest='recipe',
    )
    parser.add_argument(
        '--employee', type=int, dest='employee',
    )

    args = parser.parse_args()

    if args.insert_data:
        insert_data(cur, conn)
    if args.aliment_available:
        aliment_available(cur, args.aliment_available)
    if args.restaurant_order:
        check_command_restaurant(cur, args.restaurant_order, args.state)
    if args.customer_order:
        check_command_customers(cur, args.customer_order, args.state)
    if args.aliment_order:
        show_aliment_order(cur, args.aliment_order)
    if args.recipe:
        show_recipe(cur, args.recipe)
    if args.employee:
        show_employee(cur, args.employee)

    conn.close()


main()
