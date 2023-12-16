from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import config

app = Flask(__name__)

# Read credentials
dbname = config.postgres_credentials['dbname']
user = config.postgres_credentials['user']
password = config.postgres_credentials['password']
host = config.postgres_credentials['host']


def run_query(query, parameters=None):
    """Execute a SQL query and return the results."""
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    cursor = conn.cursor()

    try:
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)

        result = cursor.fetchall()
        return result
    finally:
        cursor.close()
        conn.close()


def execute_update(query, parameters=None):
    """Execute an update or insert or delete query."""
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    cursor = conn.cursor()

    try:
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)

        conn.commit()
    finally:
        cursor.close()
        conn.close()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/query', methods=['POST'])
def run_custom_query():
    # Retrieve user inputs for the query parameters
    math_ball100_range_start = request.form.get('math_ball100_range_start')
    math_ball100_range_end = request.form.get('math_ball100_range_end')
    birth_year = request.form.get('birth_year')
    sex_type = request.form.get('sex_type')

    # Build the SQL query based on user inputs
    query = "SELECT * FROM zno_data_new WHERE "
    conditions = []

    if math_ball100_range_start and math_ball100_range_end:
        conditions.append(f'"mathBall100" BETWEEN {math_ball100_range_start} AND {math_ball100_range_end}')

    if birth_year:
        conditions.append(f'"Birth" = {birth_year}')

    if sex_type:
        conditions.append(f'''"SEXTYPENAME" = '{sex_type}' ''')

    # Join conditions with AND
    query += " AND ".join(conditions)

    query += 'ORDER BY "mathBall100"'

    try:
        result = run_query(query)
        return render_template('result.html', result=result)
    except Exception as e:
        return render_template('error.html', error=str(e))


@app.route('/insert_zno_data_new', methods=['POST'])
def insert_row_participant():
    birth = request.form.get('birth')
    sex_type = request.form.get('sex_type')
    math_test_status = request.form.get('math_test_status')
    math_ball_100 = request.form.get('math_ball_100')
    math_ball_12 = request.form.get('math_ball_12')
    math_ball = request.form.get('math_ball')
    location_id = request.form.get('location_id')
    school_id = request.form.get('school_id')

    try:
        query_zno_data_new = """
            INSERT INTO zno_data_new ("Birth", "SEXTYPENAME", "mathTestStatus", "mathBall100", "mathBall12", "mathBall", "location_id", "school_id")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
        params_zno_data_new = (
            birth, sex_type, math_test_status, math_ball_100, math_ball_12, math_ball, location_id, school_id)
        execute_update(query_zno_data_new, params_zno_data_new)
        return redirect(url_for('index'))
    except Exception as e:
        return render_template('error.html', error=str(e))


@app.route('/insert_school', methods=['POST'])
def insert_row_school():
    eoname = request.form.get('eoname')
    eotypename = request.form.get('eotypename')

    try:
        insert_query = '''INSERT INTO school ("eoname", "eotypename") VALUES (%s, %s);'''
        execute_update(insert_query, (eoname, eotypename))
        return redirect(url_for('index'))
    except Exception as e:
        return render_template('error.html', error=str(e))


@app.route('/delete', methods=['POST'])
def delete_data():
    try:
        # Delete row from zno_data_new
        delete_index_zno_data_new = request.form.get('delete_index_zno_data_new')
        if delete_index_zno_data_new is not None:
            query = 'DELETE FROM zno_data_new WHERE "index" = %s'
            execute_update(query, (delete_index_zno_data_new, ))

        # Delete row from school
        delete_school_id = request.form.get('delete_school_id')
        if delete_school_id is not None:
            query = "DELETE FROM school WHERE school_id = %s"
            execute_update(query, (delete_school_id, ))

        # Delete row from location
        delete_location_id = request.form.get('delete_location_id')
        if delete_location_id is not None:
            query = "DELETE FROM location_mapping WHERE location_id = %s"
            execute_update(query, (delete_location_id, ))

        return redirect(url_for('index'))

    except Exception as e:
        # Handle exceptions (e.g., log the error, display an error message to the user)
        print(f"Error: {e}")
        return render_template('error.html', error_message=str(e))


if __name__ == '__main__':
    app.run(debug=True)
