
import anvil.server
import requests
import psycopg2
import bcrypt
from datetime import datetime
import os
import cloudinary
import cloudinary.uploader
import cloudinary.api
import traceback


# Connect to Anvil using the Uplink key
anvil.server.connect("server_S5LMMJKF72T7BTYDU6W3SCLQ-XFUY32F4IMRVXCL3")
# # API Configuration
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"
COUNTRY_LIST = {
    "AFN": ("Afghanistan", "https://flagcdn.com/w320/af.png"),
    "EUR": ("Euro Member Countries", "https://flagcdn.com/w320/eu.png"),
    "ALL": ("Albania", "https://flagcdn.com/w320/al.png"),
    "DZD": ("Algeria", "https://flagcdn.com/w320/dz.png"),
    "USD": ("United States", "https://flagcdn.com/w320/us.png"),
    "AOA": ("Angola", "https://flagcdn.com/w320/ao.png"),
    "XCD": ("Anguilla", "https://flagcdn.com/w320/ai.png"),
    "ARS": ("Argentina", "https://flagcdn.com/w320/ar.png"),
    "AMD": ("Armenia", "https://flagcdn.com/w320/am.png"),
    "AWG": ("Aruba", "https://flagcdn.com/w320/aw.png"),
    "AUD": ("Australia", "https://flagcdn.com/w320/au.png"),
    "AZN": ("Azerbaijan", "https://flagcdn.com/w320/az.png"),
    "BSD": ("Bahamas", "https://flagcdn.com/w320/bs.png"),
    "BHD": ("Bahrain", "https://flagcdn.com/w320/bh.png"),
    "BDT": ("Bangladesh", "https://flagcdn.com/w320/bd.png"),
    "BBD": ("Barbados", "https://flagcdn.com/w320/bb.png"),
    "BYN": ("Belarus", "https://flagcdn.com/w320/by.png"),
    "BZD": ("Belize", "https://flagcdn.com/w320/bz.png"),
    "XOF": ("Benin", "https://flagcdn.com/w320/bj.png"),
    "BMD": ("Bermuda", "https://flagcdn.com/w320/bm.png"),
    "BTN": ("Bhutan", "https://flagcdn.com/w320/bt.png"),
    "BOB": ("Bolivia", "https://flagcdn.com/w320/bo.png"),
    "BAM": ("Bosnia and Herzegovina", "https://flagcdn.com/w320/ba.png"),
    "BWP": ("Botswana", "https://flagcdn.com/w320/bw.png"),
    "BRL": ("Brazil", "https://flagcdn.com/w320/br.png"),
    "BND": ("Brunei", "https://flagcdn.com/w320/bn.png"),
    "BGN": ("Bulgaria", "https://flagcdn.com/w320/bg.png"),
    "BIF": ("Burundi", "https://flagcdn.com/w320/bi.png"),
    "KHR": ("Cambodia", "https://flagcdn.com/w320/kh.png"),
    "XAF": ("Cameroon", "https://flagcdn.com/w320/cm.png"),
    "CAD": ("Canada", "https://flagcdn.com/w320/ca.png"),
    "CVE": ("Cape Verde", "https://flagcdn.com/w320/cv.png"),
    "KYD": ("Cayman Islands", "https://flagcdn.com/w320/ky.png"),
    "CLP": ("Chile", "https://flagcdn.com/w320/cl.png"),
    "CNY": ("China", "https://flagcdn.com/w320/cn.png"),
    "COP": ("Colombia", "https://flagcdn.com/w320/co.png"),
    "KMF": ("Comoros", "https://flagcdn.com/w320/km.png"),
    "CDF": ("Congo (Kinshasa)", "https://flagcdn.com/w320/cd.png"),
    "CRC": ("Costa Rica", "https://flagcdn.com/w320/cr.png"),
    "HRK": ("Croatia", "https://flagcdn.com/w320/hr.png"),
    "CUP": ("Cuba", "https://flagcdn.com/w320/cu.png"),
    "CZK": ("Czech Republic", "https://flagcdn.com/w320/cz.png"),
    "DKK": ("Denmark", "https://flagcdn.com/w320/dk.png"),
    "DJF": ("Djibouti", "https://flagcdn.com/w320/dj.png"),
    "DOP": ("Dominican Republic", "https://flagcdn.com/w320/do.png"),
    "EGP": ("Egypt", "https://flagcdn.com/w320/eg.png"),
    "ERN": ("Eritrea", "https://flagcdn.com/w320/er.png"),
    "ETB": ("Ethiopia", "https://flagcdn.com/w320/et.png"),
    "FJD": ("Fiji", "https://flagcdn.com/w320/fj.png"),
    "GMD": ("Gambia", "https://flagcdn.com/w320/gm.png"),
    "GEL": ("Georgia", "https://flagcdn.com/w320/ge.png"),
    "GHS": ("Ghana", "https://flagcdn.com/w320/gh.png"),
    "GIP": ("Gibraltar", "https://flagcdn.com/w320/gi.png"),
    "GTQ": ("Guatemala", "https://flagcdn.com/w320/gt.png"),
    "GNF": ("Guinea", "https://flagcdn.com/w320/gn.png"),
    "GYD": ("Guyana", "https://flagcdn.com/w320/gy.png"),
    "HTG": ("Haiti", "https://flagcdn.com/w320/ht.png"),
    "HNL": ("Honduras", "https://flagcdn.com/w320/hn.png"),
    "HKD": ("Hong Kong", "https://flagcdn.com/w320/hk.png"),
    "HUF": ("Hungary", "https://flagcdn.com/w320/hu.png"),
    "ISK": ("Iceland", "https://flagcdn.com/w320/is.png"),
    "INR": ("India", "https://flagcdn.com/w320/in.png"),
    "IDR": ("Indonesia", "https://flagcdn.com/w320/id.png"),
    "IRR": ("Iran", "https://flagcdn.com/w320/ir.png"),
    "IQD": ("Iraq", "https://flagcdn.com/w320/iq.png"),
    "ILS": ("Israel", "https://flagcdn.com/w320/il.png"),
    "JMD": ("Jamaica", "https://flagcdn.com/w320/jm.png"),
    "JPY": ("Japan", "https://flagcdn.com/w320/jp.png"),
    "JOD": ("Jordan", "https://flagcdn.com/w320/jo.png"),
    "KZT": ("Kazakhstan", "https://flagcdn.com/w320/kz.png"),
    "KES": ("Kenya", "https://flagcdn.com/w320/ke.png"),
    "KWD": ("Kuwait", "https://flagcdn.com/w320/kw.png"),
    "KGS": ("Kyrgyzstan", "https://flagcdn.com/w320/kg.png"),
    "LAK": ("Laos", "https://flagcdn.com/w320/la.png"),
    "LBP": ("Lebanon", "https://flagcdn.com/w320/lb.png"),
    "LSL": ("Lesotho", "https://flagcdn.com/w320/ls.png"),
    "LRD": ("Liberia", "https://flagcdn.com/w320/lr.png"),
    "LYD": ("Libya", "https://flagcdn.com/w320/ly.png"),
    "MOP": ("Macau", "https://flagcdn.com/w320/mo.png"),
    "MKD": ("Macedonia", "https://flagcdn.com/w320/mk.png"),
    "MGA": ("Madagascar", "https://flagcdn.com/w320/mg.png"),
    "MWK": ("Malawi", "https://flagcdn.com/w320/mw.png"),
    "MYR": ("Malaysia", "https://flagcdn.com/w320/my.png"),
    "MVR": ("Maldives", "https://flagcdn.com/w320/mv.png"),
    "MRO": ("Mauritania", "https://flagcdn.com/w320/mr.png"),
    "MUR": ("Mauritius", "https://flagcdn.com/w320/mu.png"),
    "MXN": ("Mexico", "https://flagcdn.com/w320/mx.png"),
    "MDL": ("Moldova", "https://flagcdn.com/w320/md.png"),
    "MNT": ("Mongolia", "https://flagcdn.com/w320/mn.png"),
    "MAD": ("Morocco", "https://flagcdn.com/w320/ma.png"),
    "MZN": ("Mozambique", "https://flagcdn.com/w320/mz.png"),
    "MMK": ("Myanmar (Burma)", "https://flagcdn.com/w320/mm.png"),
    "NAD": ("Namibia", "https://flagcdn.com/w320/na.png"),
    "NPR": ("Nepal", "https://flagcdn.com/w320/np.png"),
    "ANG": ("Netherlands Antilles", "https://flagcdn.com/w320/an.png"),
    "NZD": ("New Zealand", "https://flagcdn.com/w320/nz.png"),
    "NIO": ("Nicaragua", "https://flagcdn.com/w320/ni.png"),
    "NGN": ("Nigeria", "https://flagcdn.com/w320/ng.png"),
    "KPW": ("North Korea", "https://flagcdn.com/w320/kp.png"),
    "NOK": ("Norway", "https://flagcdn.com/w320/no.png"),
    "OMR": ("Oman", "https://flagcdn.com/w320/om.png"),
    "PKR": ("Pakistan", "https://flagcdn.com/w320/pk.png"),
    "PAB": ("Panama", "https://flagcdn.com/w320/pa.png"),
    "PGK": ("Papua New Guinea", "https://flagcdn.com/w320/pg.png"),
    "PYG": ("Paraguay", "https://flagcdn.com/w320/py.png"),
    "PEN": ("Peru", "https://flagcdn.com/w320/pe.png"),
    "PHP": ("Philippines", "https://flagcdn.com/w320/ph.png"),
    "PLN": ("Poland", "https://flagcdn.com/w320/pl.png"),
    "QAR": ("Qatar", "https://flagcdn.com/w320/qa.png"),
    "RON": ("Romania", "https://flagcdn.com/w320/ro.png"),
    "RUB": ("Russia", "https://flagcdn.com/w320/ru.png"),
    "RWF": ("Rwanda", "https://flagcdn.com/w320/rw.png"),
    "SHP": ("Saint Helena", "https://flagcdn.com/w320/sh.png"),
    "WST": ("Samoa", "https://flagcdn.com/w320/ws.png"),
    "STN": ("São Tomé and Príncipe", "https://flagcdn.com/w320/st.png"),
    "SAR": ("Saudi Arabia", "https://flagcdn.com/w320/sa.png"),
    "RSD": ("Serbia", "https://flagcdn.com/w320/rs.png"),
    "SCR": ("Seychelles", "https://flagcdn.com/w320/sc.png"),
    "SLL": ("Sierra Leone", "https://flagcdn.com/w320/sl.png"),
    "SGD": ("Singapore", "https://flagcdn.com/w320/sg.png"),
    "SBD": ("Solomon Islands", "https://flagcdn.com/w320/sb.png"),
    "SOS": ("Somalia", "https://flagcdn.com/w320/so.png"),
    "ZAR": ("South Africa", "https://flagcdn.com/w320/za.png"),
    "KRW": ("South Korea", "https://flagcdn.com/w320/kr.png"),
    "SSP": ("South Sudan", "https://flagcdn.com/w320/ss.png"),
    "LKR": ("Sri Lanka", "https://flagcdn.com/w320/lk.png"),
    "SDG": ("Sudan", "https://flagcdn.com/w320/sd.png"),
    "SRD": ("Suriname", "https://flagcdn.com/w320/sr.png"),
    "SZL": ("Swaziland", "https://flagcdn.com/w320/sz.png"),
    "SEK": ("Sweden", "https://flagcdn.com/w320/se.png"),
    "CHF": ("Switzerland", "https://flagcdn.com/w320/ch.png"),
    "SYP": ("Syria", "https://flagcdn.com/w320/sy.png"),
    "TWD": ("Taiwan", "https://flagcdn.com/w320/tw.png"),
    "TJS": ("Tajikistan", "https://flagcdn.com/w320/tj.png"),
    "TZS": ("Tanzania", "https://flagcdn.com/w320/tz.png"),
    "THB": ("Thailand", "https://flagcdn.com/w320/th.png"),
    "TOP": ("Tonga", "https://flagcdn.com/w320/to.png"),
    "TTD": ("Trinidad and Tobago", "https://flagcdn.com/w320/tt.png"),
    "TND": ("Tunisia", "https://flagcdn.com/w320/tn.png"),
    "TRY": ("Turkey", "https://flagcdn.com/w320/tr.png"),
    "TMT": ("Turkmenistan", "https://flagcdn.com/w320/tm.png"),
    "UGX": ("Uganda", "https://flagcdn.com/w320/ug.png"),
    "UAH": ("Ukraine", "https://flagcdn.com/w320/ua.png"),
    "AED": ("United Arab Emirates", "https://flagcdn.com/w320/ae.png"),
    "UYU": ("Uruguay", "https://flagcdn.com/w320/uy.png"),
    "UZS": ("Uzbekistan", "https://flagcdn.com/w320/uz.png"),
    "VUV": ("Vanuatu", "https://flagcdn.com/w320/vu.png"),
    "VEF": ("Venezuela", "https://flagcdn.com/w320/ve.png"),
    "VND": ("Vietnam", "https://flagcdn.com/w320/vn.png"),
    "XPF": ("Wallis and Futuna", "https://flagcdn.com/w320/wf.png"),
    "YER": ("Yemen", "https://flagcdn.com/w320/ye.png"),
    "ZMW": ("Zambia", "https://flagcdn.com/w320/zm.png"),
    "ZWL": ("Zimbabwe", "https://flagcdn.com/w320/zw.png")
}


# Cloudinary configuration
cloudinary.config(
  cloud_name = 'ds310f9hg',
  api_key = '247315982122596',
  api_secret = 'ihFtLaq4IHFMhBDqTWvVAMvzAik'
)
# Database connection parameters with SSL enabled
conn_params = {
    'dbname': 'Dupay',
    'user': 'gtpl',
    'password': 'mu6-f6rSv9t_oCLSh6iFLg',
    'host': 'meteor-dunnart-5620.7s5.aws-ap-south-1.cockroachlabs.cloud',
    'port': '26257',
    'sslmode': 'require'  # Use SSL mode 'require'
}


def get_db_connection():
    """Establishes a new database connection."""
    return psycopg2.connect(**conn_params)


@anvil.server.callable
def get_users_from_db():
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cur = conn.cursor()
        cur.execute("""SELECT 
                user_fullname, 
                user_phone_number, 
                user_type, 
                user_email,
                user_profile_photo,
                user_inactive,
                user_hold
            FROM users""")
        users = cur.fetchall()

        if not users:
            print("No users found in the database.")
        else:
            print(f"Fetched users: {users}")  # Log the fetched data

        cur.close()
        conn.close()
        return users

    except Exception as e:
        print(f"Error fetching users: {e}")
        traceback.print_exc()  # Print full exception traceback
        return None
def get_user_details(phone_number):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Query to fetch user details
        query = """
        SELECT user_fullname, user_email, user_aadhar_number, user_pan_number, user_phone_number, user_address_line_1, user_country
        FROM users
        WHERE user_phone_number = %s;
        """
        cur.execute(query, (phone_number,))
        
        # Fetch the data
        user_data = cur.fetchone()
        cur.close()
        conn.close()
        
        if user_data:
            return {
                'user_fullname': user_data[0],
                'user_email': user_data[1],
                'user_aadhar_number': user_data[2],
                'user_pan_number': user_data[3],
                'user_phone_number': user_data[4],
                'user_address_line_1': user_data[5],
                'user_country': user_data[6]
            }
        else:
            return None
        
    except Exception as e:
        print("Error occurred while fetching user details:", e)
        return None

@anvil.server.callable
def get_user_details_by_phone(phone_number):
    return get_user_details(phone_number)

def fetch_currency_data():
    """Fetches currency data from the API."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json().get('rates', {})
    except requests.RequestException as e:
        print(f"Error fetching currency data: {e}")
        return {}
def add_currency_to_db(currency_code, country_name, icon_url):
    """Inserts currency data into the database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if the entry already exists
        cur.execute(
            "SELECT * FROM admins_add_currency WHERE admins_add_currency_code = %s",
            (currency_code,)
        )
        existing_entry = cur.fetchone()

        if existing_entry:
            print(f"Currency {currency_code} already exists.")
            cur.close()
            conn.close()
            return

        # Insert the new currency with the icon path
        sql = """
        INSERT INTO admins_add_currency (admins_add_currency_country, admins_add_currency_code, admins_add_currency_icon)
        VALUES (%s, %s, %s)
        """
        cur.execute(sql, (country_name, currency_code, icon_url))
        conn.commit()

        print(f"Currency {currency_code} added successfully.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error adding currency to database: {e}")

def fetch_and_store_currencies():
    """Fetches currency data from the API and stores it in the database."""
    currency_data = fetch_currency_data()
    for currency_code, _ in currency_data.items():
        # Fetch country name and icon URL using the correct tuple structure
        country_info = COUNTRY_LIST.get(currency_code)
        
        if country_info is None:
            print(f"No information found for currency code: {currency_code}")
            continue
        
        country_name, icon_url = country_info

        # Debugging to confirm the values before storing
        print(f"Storing data - Currency Code: {currency_code}, Country Name: {country_name}, Icon URL: {icon_url}")

        # Store the details in the database
        add_currency_to_db(currency_code, country_name, icon_url)

if __name__ == "__main__":
    fetch_and_store_currencies()
# SQL query to create the admin_add_currency table
def create_table():
    try:
        conn =  get_db_connection()
        cur = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS admins_add_currency (
            id SERIAL PRIMARY KEY,
            admins_add_currency_code VARCHAR(3) NOT NULL,
            admins_add_currency_country VARCHAR(255) NOT NULL,
            admins_add_currency_icon VARCHAR(1024) NOT NULL
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
        conn.close()

        print("Table admins_add_currency created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

# create_table()

@anvil.server.callable
def get_currency_data():
    try:
        conn =  get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT admins_add_currency_country, admins_add_currency_code ,admins_add_currency_icon FROM admins_add_currency")
        currency_data = cur.fetchall()

        cur.close()
        conn.close()

        return [{"country": row[0], "code": row[1],"icon" : row[2]} for row in currency_data]
    except Exception as e:
        print(f"Error fetching currency data: {e}")
        return []

@anvil.server.callable
def delete_currency_by_code(currency_code):
    """Deletes a currency type from the database based on currency code."""
    try:
        conn =  get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            DELETE FROM admins_add_currency
            WHERE admins_add_currency_code = %s
        """, (currency_code,))
        
        deleted_count = cur.rowcount
        conn.commit()

        cur.close()
        conn.close()

        return deleted_count > 0
    except Exception as e:
        print(f"Error deleting currency: {e}")
        raise e

@anvil.server.callable
def add_currency(country_name, currency_code, currency_icon_media):
    try:
        file_name = f"{currency_code}_{country_name}.png"
        local_file_path = os.path.join('media/profile_photos', file_name)

        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        # Save the image locally
        with open(local_file_path, "wb") as f:
            f.write(currency_icon_media.get_bytes())

        # Upload the image to Cloudinary
        cloudinary_response = cloudinary.uploader.upload(local_file_path, folder='profile_photos')
        cloudinary_url = cloudinary_response.get('url')

        # Store the file path in the database
        conn =  get_db_connection()
        cur = conn.cursor()

        # Check if the entry already exists
        cur.execute(
            "SELECT * FROM admins_add_currency WHERE admins_add_currency_country = %s AND admins_add_currency_code = %s",
            (country_name, currency_code)
        )
        existing_entry = cur.fetchone()

        if existing_entry:
            cur.close()
            conn.close()
            return False, "Country with this currency code already exists"

        # Insert the new currency with the icon path
        sql = """
        INSERT INTO admins_add_currency (admins_add_currency_country, admins_add_currency_code, admins_add_currency_icon)
        VALUES (%s, %s, %s)
        """
        data = (country_name, currency_code, cloudinary_url)  # Store the file path as a string

        cur.execute(sql, data)
        conn.commit()

        cur.close()
        conn.close()

        return True, "Currency added successfully"
    except Exception as e:
        print(f"Error adding currency: {e}")  # Log the error message
        return False, f"Failed to add currency: {e}"


@anvil.server.callable
def check_phone_number_exists(phone_number):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Check if phone number exists in the database
    cur.execute("SELECT 1 FROM users WHERE user_phone_number = %s", (phone_number,))
    phone_exists = cur.fetchone() is not None

    cur.close()
    conn.close()

    return phone_exists


@anvil.server.callable
def generate_user_id():
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Fetch the last generated user_id
    cur.execute("SELECT user_id FROM users WHERE user_id LIKE 'dupA%' ORDER BY user_id DESC LIMIT 1;")
    last_user_id = cur.fetchone()

    if last_user_id:
        last_id_number = int(last_user_id[0][4:])  # Extract the numeric part after "dupA"
        new_id_number = last_id_number + 1
    else:
        new_id_number = 1  # Start with 1 if no users exist

    # Generate new user_id with the pattern dupA0001, dupA0002, etc.
    new_user_id = f"dupA{new_id_number:04d}"

    cur.close()
    conn.close()

    return new_user_id



@anvil.server.callable
def add_admins_info(user_id, fullname, email, phone_number, password, user_dob, user_gender, user_joined_date):
    print(f"user_dob: {user_dob}, user_joined_date: {user_joined_date}")

    # Ensure dates are in the correct format
    if isinstance(user_dob, str):
        user_dob = datetime.strptime(user_dob, '%Y-%m-%d').date()
    if isinstance(user_joined_date, str):
        user_joined_date = datetime.strptime(user_joined_date, '%Y-%m-%d').date()

    # Hash the password using bcrypt with a cost factor of 12
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO users (user_id, user_fullname, user_email, user_phone_number, user_password, user_dob, user_gender, user_joined_date, user_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'admin')
            RETURNING user_id;
            """,
            (user_id, fullname, email, phone_number, hashed_password, user_dob, user_gender, user_joined_date)
        )
        admin_id = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

    return f"Admin added successfully with ID {admin_id}!"

@anvil.server.callable
def login_user(email, password):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Fetch user data based on email
    cur.execute("SELECT user_password, user_id, user_fullname, user_type FROM users WHERE user_email = %s", (email,))
    user = cur.fetchone()

    cur.close()
    conn.close()

    if user:
        hashed_password = user[0]

        # Verify the entered password against the stored hashed password
        if bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8')):
            return {
                'user_id': user[1],
                'user_fullname': user[2],
                'user_email': email,
                'user_type': user[3]
            }
        else:
            return None
    else:
        return None


@anvil.server.callable
def check_email_exists(email):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Check if email exists in the database
    cur.execute("SELECT 1 FROM users WHERE user_email = %s", (email,))
    user_exists = cur.fetchone() is not None

    cur.close()
    conn.close()

    return user_exists

@anvil.server.callable
def get_user_for_login(login_input):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Fetch user by email
    cur.execute("SELECT user_email, user_password, user_type FROM users WHERE user_email = %s", (login_input,))
    user = cur.fetchone()

    cur.close()
    conn.close()

    if user:
        # Return user data as a dictionary
        return {
            'user_email': user[0],
            'user_password': user[1],
            'user_type': user[2],
        }
    
    return "User not found"
    
# Keep the script running
anvil.server.wait_forever()
