"""
A python module for entering student information
"""

# Import sqlite3 and matplotlib libraries
import sqlite3
import matplotlib.pyplot as plt

db = sqlite3.connect('test1.db')
cursor = db.cursor()


# Start of the program
def create():
    """
    Prompts the user to create a table before beginning
    :return: Brings the user back to the creation menu
    """
    print("Welcome to the beginning of the program. Let's start by creating a table.")
    try:
        tName = input("Enter new table name: ")
        sql_query = "CREATE TABLE %s (\
                 student_id INTEGER PRIMARY KEY AUTOINCREMENT, \
                 student_name TEXT NOT NULL, \
                 student_grade INTEGER, \
                 course_code INTEGER, \
                 student_number INTEGER, \
                 CONSTRAINT student_grade_check CHECK (student_grade between 0 and 100), \
                 CONSTRAINT course_code_check CHECK (course_code between 1 and 9999), \
                 CONSTRAINT student_number_check CHECK (student_number >= 1))" % (tName)
        cursor.execute(sql_query)
        db.commit()
        print("Table successfully created!")
    except sqlite3.OperationalError:
        print("Table name either exists or is invalid.")
        return create()


# Main Menu
def start():
    """
    Gives the user a range of options to start off
    :return: Brings the user back to the main menu
    """
    # Prompts the user to make a choice
    main_menu = """Welcome to the main menu. Choose one of the options listed below:


     1) View a selected table.
     2) Add names to a selected table.
     3) Delete info from a selected table.
     4) Create a new table.
     5) Remove an existing table.
     6) Graph a table.
     7) Exit the program.


     """
    print(main_menu)
    functions = {
        '1': view,
        '2': check,
        '3': delete,
        '4': form,
        '5': remove,
        '6': graph,
        '7': exit
    }
    res = input("What will it be: ")
    if res in functions:
        functions[res]()
    else:
        return start()


def check():
    # Displays all the current tables
    cursor.execute('SELECT name from sqlite_master where type= "table"')
    print("Here are you current tables.")
    print(cursor.fetchall())
    # Does not allow the user to add names to invalid or non-existent tables
    try:
        # Prompts the user to enter the table they would like to add name to
        table = input("Which table would you like to add information to: ")
        add(table)
    except sqlite3.OperationalError:
        print("Sorry but the table you have selected is either invalid or does not exist. Returning to Main Menu")
        return start()


# This function lets the user view the contents of a table
def view():
    """
    Allows the user to view the table of their choosing
    :return: Brings the user back to the main menu
    """
    # Displays all the current tables
    cursor.execute('SELECT name from sqlite_master where type= "table"')
    print("Here are you current tables.")
    print(cursor.fetchall())
    # Prompts the user the enter the table they want to see
    table = input("Enter the name of the table you would like to view: ")
    # If the table has no contents, bring the user back to the main menu
    try:
        sql_query = "SELECT student_number, student_grade, student_name, course_code\
                     FROM %s\
                     ORDER BY student_number, student_grade, student_name, course_code" % (table)
        cursor.execute(sql_query)
        all_emp_rows = cursor.fetchall()
        if all_emp_rows:
            print("\nStudent Number|Student Grade|Student Name|Course Code")
            for row in all_emp_rows:
                name = row[0]
                grade = row[1]
                course = row[2]
                id = row[3]
                print("{0}|{1}|{2}|{3}".format(name, grade, course, id))
        else:
            print("At the moment there is no information to be shown. Returning to Main Menu.")
            return start()
    # If the table is invalid, bring the user back to the main menu
    except sqlite3.OperationalError:
        print("Sorry but the file you have selected is not a valid file. Returning to Main Menu")
        return start()
    # Prompts the user to make a choice
    view_menu = """Choose one of the options listed below:


    1) Add additional info to the table.
    2) Display the average, sum, and highest of the grade in the table.
    3) Exit to main menu.


    """
    print(view_menu)
    functions = {
        '1': options,
        '2': ops,
        '3': start
    }
    try:
        inp = input("What will it be: ")
        if inp in functions:
            functions[inp](table)
        else:
            view()
    except TypeError:
        return start()


# This gives the user different options to add to a student
def options(table):
    """
    Allows the user to give a specified student either a grade, course code, or ID number (ALLOCATION MENU)
    :return: Brings user back to the database
    """
    # Prompts the user to make a choice
    options_menu = """Choose one of the options listed below:


    1) Give a chosen student a grade. 
    2) Give a chosen student a course code.
    3) Give a chosen student an ID number.
    4) Display the average, sum, and highest of the grade in the table.
    5) Exit to the table menu.


    """
    print(options_menu)
    functions = {
        '1': grade,
        '2': cCode,
        '3': id,
        '4': ops,
        '5': view
    }
    ans = input("What will it be: ")
    try:
        if ans in functions:
            functions[ans](table)
        else:
            return options(table)
    except TypeError:
        return view()


# Adds a grade to a student
def grade(table):
    """
    Allows the user to give a student a grade
    :return: Brings the user back the allocation menu
    """
    # Displays all the current students in the table
    print("Here is the name of all the students.")
    sql_query_1 = "SELECT student_name \
                   FROM %s" % (table)
    cursor.execute(sql_query_1)
    emp_row = cursor.fetchall()
    for row in emp_row:
        print(row)
    # Prompts the user to enter the name of the student whose grade they want changed
    name = input("Enter the name of the student whose grade you would like to change: ")
    sql_query = "SELECT student_id \
    		     FROM %s \
                 WHERE student_name = ?" % (table)
    cursor.execute(sql_query, (name,))
    name_row = cursor.fetchone()
    # If the name is in the table, then prompt for the user to give the student a grade
    if name_row:
        named = name_row[0]
        # Loop used to ensure the value entered is a number
        while True:
            num = input("Enter the grade you would like to give the student: ")
            try:
                grade = int(num)
                break;
            except ValueError:
                try:
                    float(num)
                    break;
                except ValueError:
                    print("Please enter a number grade: ")
        # The grade has to be between 0 and 100
        if grade >= 0 and grade <= 100:
            sql_update = "UPDATE %s \
                          SET student_grade = (?) \
                          WHERE student_name = (?)" % (table)
            cursor.execute(sql_update, (grade, name))
            db.commit()
            print("Grade successfully added!")
        else:
            print("Grade has to be between 0 and 100.")
            return options(table)
    # If the student is not in the table then return to the option function
    else:
        print("Student not in database.")
        return options(table)
    return options(table)


# Adds a course code to a student
def cCode(table):
    """
    Allows the user to assign a student a course code
    :return: Brings the user back the allocation menu
    """
    # Displays all of the current students in the table
    print("Here is the name of all the students.")
    sql_query_1 = "SELECT student_name \
                   FROM %s" % (table)
    cursor.execute(sql_query_1)
    emp_row = cursor.fetchall()
    for row in emp_row:
        print(row)
    # Prompts the user to enter the name of the student whose course cod they want changed
    name = input("Enter the name of the student whose course code you would like to change: ")
    sql_query = "SELECT student_id \
                 FROM %s \
                 WHERE student_name = ?" % (table)
    cursor.execute(sql_query, (name,))
    name_row = cursor.fetchone()
    # If the name is in the table, then prompt the user for a course code
    if name_row:
        named = name_row[0]
        # Loop used to ensure the value entered is a number
        while True:
            num = input("Enter the course code you would like to give the student: ")
            try:
                code = int(num)
                break;
            except ValueError:
                try:
                    float(num)
                    break;
                except ValueError:
                    print("Please enter a numerical code")
        # The course code has to be between 1 and 9999
        if code >= 1 and code <= 9999:
            sql_update = "UPDATE %s \
                          SET course_code = (?) \
                          WHERE student_name = (?)" % (table)
            cursor.execute(sql_update, (code, name))
            db.commit()
            print("Course code successfully added!")
        else:
            print("Course code has to be between 1 and 9999.")
            return options(table)
    # If the student is not in the database, return to the options function
    else:
        print("Student not in database.")
        return options(table)
    return options(table)


# Adds an ID to a student
def id(table):
    """
    Allows the user to give a student an ID
    :return: Brings the user back the allocation menu
    """
    # Displays all the current students in the table
    print("Here is the name of all the students.")
    sql_query_1 = "SELECT student_name \
                   FROM %s" % (table)
    cursor.execute(sql_query_1)
    emp_row = cursor.fetchall()
    for row in emp_row:
        print(row)
    # Prompts the user to enter the name of the student whose ID number they want changed
    name = input("Enter the name of the student whose ID number you would like to change: ")
    sql_query = "SELECT student_id \
               	  FROM %s \
                 WHERE student_name = ?" % (table)
    cursor.execute(sql_query, (name,))
    name_row = cursor.fetchone()
    # If the student is in the table, prompts the user for an ID number
    if name_row:
        named = name_row[0]
        # Loop used to ensure the value entered is a number
        while True:
            num = input("Enter the grade you would like to give the student: ")
            try:
                id = int(num)
                break;
            except ValueError:
                try:
                    float(num)
                    break;
                except ValueError:
                    print("Please enter a numerical ID: ")
        # Ensures that the ID number entered is greater than 1
        if id >= 1:
            sql_update = "UPDATE %s \
                          SET student_number = (?) \
                          WHERE student_name = (?)" % (table)
            cursor.execute(sql_update, (id, name))
            db.commit()
            print("ID successfully added!")
        else:
            print("ID has to be greater than 1.")
            return options(table)
    # If the student is not in the table, return to the options table
    else:
        print("Student not in table.")
        return options(table)
    return options(table)


# Displays the average, sum, and highest grade of a table
def ops(table):
    """
    Allows the user to see the average, sum, and highest of the grades in a table
    :return: Brings the user back to the option menu
    """
    sql_query1 = "SELECT SUM(student_grade) \
                      FROM %s" % (table)
    cursor.execute(sql_query1)
    emp_row1 = cursor.fetchone()
    for row in emp_row1:
        print("The current sum is " + str(row))
    sql_query2 = "SELECT AVG(student_grade) \
                      FROM %s" % (table)
    cursor.execute(sql_query2)
    emp_row2 = cursor.fetchone()
    for row in emp_row2:
        print("The current average is " + str(row))
    sql_query3 = "SELECT student_name, MAX(student_grade) \
                      FROM %s" % (table)
    cursor.execute(sql_query3)
    emp_row3 = cursor.fetchall()
    for row in emp_row3:
        student_name = row[0]
        student_grade = row[1]
        print('The student with the highest grade {0} with {1}'.format(student_name, student_grade))
    # After displaying everything, prompts the user to make a choice
    ops_menus = """Choose one of the options listed below:


        1) Add additional info to the table.
        2) Exit to table menu.


        """
    print(ops_menus)
    ans = input("What will it be: ")
    # Brings the user to the option function
    if ans == '1':
        options(table)
    # Brings the user to the view function
    if ans == '2':
        return view()
    # Brings the user back to the ops function
    else:
        return ops(table)


# Adds names to a table
def add(table):
    """
    Allows the user to add names to store into a file
    :return: Brings the user back to the main menu
    """
    # Prompts the user to make a choice
    add_menu = """Choose one of the options listed below:


     1) Add names to the table selected.
     2) Exit to main menu.


    """
    print(add_menu)
    res = input("What will it be: ")
    # Prompts the user to type in a name into the table
    if res == '1':
        sName = input("Please enter a student name: ")
        # Inserts the name into the student name column of the table selected
        sql_insert = "INSERT INTO %s(student_name) \
                              VALUES(?)" % (table)
        cursor.execute(sql_insert, (sName,))
        print("Inserted new name!")
        db.commit()
        # Reruns the the function
        return add(table)
    # Brings the user back to the main menu
    elif res == '2':
        return start()
    # Brings the user back to the add function
    else:
        return add(table)


# This function lets the user make another table
def form():
    """
    This function allows the user to create a new table
    :return: Brings user back to the main menu
    """
    # This gives the user the optionto either create a table or go back to the main menu
    form_menu = """Choose one of the options listed below:


     1) Create a table.
     2) Exit to main menu.


    """
    print(form_menu)
    ans = input("What will it be: ")
    if ans == '1':
        # This query adds a primary key along with name, grade, course code, and number columns
        try:
            tName = input("Enter new table name: ")
            sql_query = "CREATE TABLE %s (\
                     student_id INTEGER PRIMARY KEY AUTOINCREMENT, \
                     student_name TEXT NOT NULL, \
                     student_grade INTEGER, \
                     course_code INTEGER, \
                     student_number INTEGER, \
                     CONSTRAINT student_grade_check CHECK (student_grade between 0 and 100), \
                     CONSTRAINT course_code_check CHECK (course_code between 1 and 9999), \
                     CONSTRAINT student_number_check CHECK (student_number >= 1))" % (tName)
            cursor.execute(sql_query)
            db.commit()
            print("Table successfully added to database!")
        # If there is an error, rerun the function
        except sqlite3.OperationalError:
            print("Table name either exists or is invalid.")
            return form()
    # Brings the user back to the main menu
    elif ans == '2':
        return start()
    # Reruns the function
    else:
        return form()


# Deletes information from a table
def delete():
    """
    Allows the user to delete one of their entries in the table selected
    :return: Brings the user back to the main menu
    """
    # Prompts the user to make a choice
    delete_menu = """Choose one of the options listed below:


     1) Delete an entry from a table.
     2) Exit to main menu.


    """
    print(delete_menu)
    ans = input("What will it be: ")
    # Displays all current tables
    try:
        if ans == '1':
            cursor.execute('SELECT name from sqlite_master where type= "table"')
            print("Here are you current tables.")
            print(cursor.fetchall())
            # Prompts the user to enter the name of the table they want to delete information from
            table = input("Which table would you like to delete information from: ")
            # If the table does exist, then display all students in the table
            print("Here is the name of all the students.")
            sql_query_1 = "SELECT student_name \
                               FROM %s" % (table)
            cursor.execute(sql_query_1)
            emp_row = cursor.fetchall()
            for row in emp_row:
                print(row)
            # Prompts the user the enter the student they want deleted
            name = input("Enter the name of the student you wish to remove: ")
            sql_query = "SELECT student_id \
                         FROM %s \
                         WHERE student_name = ?" % (table)
            cursor.execute(sql_query, (name,))
            emp_row = cursor.fetchone()
            # If the student exists, then remove their information from the table
            if emp_row:
                id = emp_row[0]
                sql_delete = "DELETE FROM %s \
                              WHERE student_name = ?" % (table)
                cursor.execute(sql_delete, (name,))
                db.commit()
                print(name + "'s information has been deleted from the table.")
                return delete()
                # If the student does not exist then rerun the function
            else:
                print("That student is not in the table.")
                return delete()
            # Brings the user back to the main menu
        elif ans == '2':
            return start()
        # Rerun the function
        else:
            return delete()
    except sqlite3.OperationalError:
        # If there is an error, return to the main menu
        print("Sorry but the table you have selected is either invalid or does not exist. Returning to Main Menu")
        return delete()


# Removes a table from the database
def remove():
    """
    Allows the user to remove a table from the database
    :return: Brings the user back to the main menu
    """
    # Prompts the user to make a choice
    remove_menu = """Choose one of the options listed below:


     1) Remove a table from the database.
     2) Exit to main menu.


    """
    print(remove_menu)
    ans = input("What will it be: ")
    # Displays all current tables
    if ans == '1':
        cursor.execute('SELECT name from sqlite_master where type= "table"')
        print("Here are you current tables.")
        print(cursor.fetchall())
        # Prompts the use to enter the tale they want deleted
        try:
            tName = input("Enter the table name you wish to delete: ")
        # If there is an error, rerun the function
        except sqlite3.OperationalError:
            print("Sorry but the table you have selected is either invalid or does not exist. Returning to Main Menu")
            return remove()
        # If the table selected is sqlite_sequence, tell the user it can not be dropped
        if tName == 'sqlite_sequence':
            print("Table sqlite_sequence may not be dropped")
            return remove()
        # Drop the table selected
        else:
            sql_query = "DROP TABLE IF EXISTS %s" % (tName)
            cursor.execute(sql_query)
            db.commit()
            print("Table successfully removed from database!")
            start()
    # Brings the user back to the main menu
    elif ans == '2':
        return start()
    return remove()


# Graphs a table
def graph():
    """
    Allows the user to graph names and grades of a table
    :return: Brings the user back to the main menu
    """
    # Prompts the user to make a choice
    graph_menu = """Choose one of the options listed below:


     1) Graph the names and grades from the table selected.
     2) Exit to main menu.


    """
    print(graph_menu)
    ans = input("What will it be: ")
    # Displays all current tables
    try:
        if ans == '1':
            cursor.execute('SELECT name from sqlite_master where type= "table"')
            print("Here are your current tables.")
            print(cursor.fetchall())
            # Prompts the user to enter the name of the table they want to graph
            table = input("Enter the name of the table you would like to graph: ")
            sql_query = "SELECT student_name, student_grade \
                         FROM %s" % (table)
            cursor.execute(sql_query)
            all_emp_rows = cursor.fetchall()
            names = []
            grades = []
            for emp_row in all_emp_rows:
                names.append(emp_row[0])
                grades.append(emp_row[1])
            # What will be graphed, what colors are used, title, axes names, and fontsizes
            color_coat = ['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'black', 'brown', 'gold', 'silver',
                          'pink']
            plt.title(table, fontsize=17)
            plt.ylabel('Grades', fontsize=15)
            plt.xlabel('Students', fontsize=15)
            plt.bar(names, grades, color=color_coat)
            plt.grid(True)
            plt.show()
            # Saves the graph as a png file
            plt.savefig('graph.png')
            print("Graph saved as graph.png")
            return graph()
        # Brings the user back to the main menu
        elif ans == '2':
            return start()
        # Rerun the function
        else:
            return graph()
    except sqlite3.OperationalError:
        print("The table you have selected is either invalid or does not exist.")
        return graph()


# Starts the program
if __name__ == "__main__":
    create()
    start()
else:
    print("Program is not ready.")