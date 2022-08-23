"""
This program moves data from ActionKit Signups (through the TX Dems volunteer page: https://action.txdemocrats.org/sign/2021-volunteer/)
to MyC VAN. It either updates or finds the MyC profile in VAN, and applies either an SRs or AC. Mappings are based
on input from the TX Dems, and are located here: https://docs.google.com/spreadsheets/d/1UWg3gJ-d95MmrxuHHdB8UR4jJZnNMV32b2g2SxOMVGg/edit#gid=0

The intended workflow is for this script to run once evey day and pipe in the data from the day before.

Change the SQL and input variables in the actionkit_to_van function, and the mappings in the find_and_create_and_apply function to
modify the function to use for other forms.
"""
import requests
import os
import re
from email_validator import validate_email, EmailNotValidError
from datetime import datetime
from stac_utils.ticker_request import TickerRequest


# # Load .env file, for local use. Make sure to enable for local use
# from dotenv import load_dotenv
# load_dotenv()


# VAN Find and Create to update MyC info, and additional code to apply SR/ACs
def find_and_create_and_apply(first_name, middle_name, last_name, city, state, zip, email, phone,
                              email_subscription_status, question_name, question_response, date_updated) -> None:
    """
    This function takes the Actionkit data input, validates and formats for Find or Create VAN endpoint,
    sends the data to the VAN endpoint, and then pipes the data to either creating an SQ or AC.

    :param first_name: str, First Name of Actionkit signup
    :param middle_name: str, Middle Name of ActionKit signup
    :param last_name: str, Last Name of ActionKit signup
    :param city: str, City of ActionKit Signup
    :param state: str, State of Actionkit Signup
    :param zip: str, zip of ActionKit Signup
    :param email: str, email of ActionKit Signup
    :param phone: str, phone number of ActionKit Signup
    :param email_subscription_status: str, latest email subscription status of ActionKit Signup
    :param question_name: str,  Survey Question from ActionKit Signup
    :param question_response: str, Survey Response from ActionKit Signup
    :param date_updated: str, last updated date from ActionKit Signup
    :return: none
    """
    # Phone Variable Validation / Handling
    phone = phone_validation(phone)

    # Email Variable Validation / Handling
    email = email_validation(email)

    # Zip code validation
    zip = zip_validation(zip)

    # All other variable validation
    city = name_and_place_validation(city)
    state = name_and_place_validation(state)
    first_name = name_and_place_validation(first_name)
    middle_name = name_and_place_validation(middle_name)
    last_name = name_and_place_validation(last_name)

    # Gender Variable VAN formatting
    sex = ""
    if question_name == "gender":
        if question_response == "Man":
            sex = "M"
        elif question_response == "Woman":
            sex = "F"

    # Email Subscription Status Variable VAN Formatting
    if email_subscription_status == "subscribed":
        subscribe_status = "S"
    elif email_subscription_status == "unsubscribed":
        subscribe_status = "U"
    else:
        subscribe_status = "N"

    # OPT-IN Status Variable VAN Formatting
    opt_in = ""
    if question_name == 'sms_subscriber':
        if question_response == 'Yes':
            opt_in = "I"

    # Payload for Find Or Create
    payload = {
        "firstName": f"{first_name}",
        "middleName": f"{middle_name}",
        "lastName": f"{last_name}",
        "sex": f"{sex}",
        "emails": [
            {
                "email": f"{email}",
                "subscriptionStatus": f"{subscribe_status}"
                # string. One of U → unsubscribed, N → neutral, S→ subscribed.
            }
        ],
        "phones": [
            {
                "phoneNumber": f"{phone}",
                "phoneOptInStatus": f"{opt_in}",
                # one of: I → opt in, U → unknown, O → opt out. Default, if not supplied, is U
            }
        ],
        "addresses": [
            {
                "city": f"{city}",
                "stateOrProvince": f"{state}",
                "zipOrPostalCode": f"{zip}"

            }
        ]
    }

    if phone == "":
        del payload["phones"]
    if email == "":
        del payload["emails"]

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": os.getenv("VAN_AUTH")
    }

    # VAN Find or Create Endpoint
    VAN_FC_ENDPOINT_URL = 'https://api.securevan.com/v4/people/findOrCreate'

    # Sending the data for Find or Create to MyC
    response = requests.post(VAN_FC_ENDPOINT_URL, json=payload, headers=headers)

    # Get the MyC VAN ID from created or found MyC profile, for SR/AC application
    dict = response.json()
    van_id = dict["vanId"]

    # Convert canvass_date into ISO 8601 format required by VAN
    date_updated_iso = datetime.fromisoformat(date_updated).isoformat()

    # At this point, I could probably create another function to better organize the code. However, it still
    # works as is.

    # Initialize SQ and SR IDs to 0
    question_name_id = 0
    question_response_id = 0

    # Based on values of ActionKit inputs question_name and question_response, get the correct SQ/SR ids, to send to VAN
    if question_name == "race":
        # Set correct SQ ID
        question_name_id = 371853
        # Set correct SR ID
        if question_response == "African American or Black":
            question_response_id = 1529977
        elif question_response == "Asian":
            question_response_id = 1529978
        elif question_response == "Hispanic or Latinx":
            question_response_id = 1529979
        elif question_response == "Middle Eastern or North African":
            question_response_id = 1529980
        elif question_response == "Native American or Alaska Native":
            question_response_id = 1529981
        elif question_response == "Native Hawaiian or Other Pacific Islander":
            question_response_id = 1529981
        elif question_response == "White":
            question_response_id = 1529982
    elif question_name == "volunteer_opportunities":
        # Set correct SQ ID
        question_name_id = 371846
        # Set correct SR ID
        if question_response == "Host an event":
            question_response_id = 1549378
        elif question_response == "Blockwalk":
            question_response_id = 1529944
        elif question_response == "Attend a local community meeting":
            question_response_id = 1549389
        elif question_response == "Data Entry":
            question_response_id = 1529945
        elif question_response == "House a staffer":
            question_response_id = 1549387
        elif question_response == "Make calls":
            question_response_id = 1529943
        elif question_response == "Text voters":
            question_response_id = 1529940
        elif question_response == "Register voters":
            question_response_id = 1549384
        elif question_response == "Serve as a poll watcher":
            question_name_id = 485979
            question_response_id = 1984071
    elif question_name == "languages":
        # Set correct SQ ID
        question_name_id = 371847
        # Set correct SR ID
        if question_response == "Other":
            question_response_id = 1529970
        elif question_response == "American Sign Language":
            question_response_id = 1529969
        elif question_response == "Arabic":
            question_response_id = 1529964
        elif question_response == "Urdu ":
            question_response_id = 1529964
        elif question_response == "Hindi, Gujarati, Punjabi, other":
            question_response_id = 1529963
        elif question_response == "Tagalog":
            question_response_id = 1529960
        elif question_response == "Mandarin or Cantonese":
            question_response_id = 1529959
        elif question_response == "Vietnamese":
            question_response_id = 1529958
        elif question_response == "Spanish":
            question_response_id = 1529957
    elif question_name == "identity":
        # Set correct SQ ID
        question_name_id = 371853
        # Set correct SR ID
        if question_response == "LGBTQ+":
            question_response_id = 1529994
        elif question_response == "Disability":
            question_response_id = 1529995
        elif question_response == "Veteran":
            question_response_id = 1529996
        elif question_response == "Youth":
            question_response_id = 1529997
        elif question_response == "Labor / Union":
            question_response_id = 1529998
        elif question_response == "Student":
            question_response_id = 1530002
        elif question_response == "Teacher":
            question_response_id = 1530003
        elif question_response == "Lawyer/Legal Professional":
            question_response_id = 4700612

    # Based on AC or SQ id value, send to apply either AC, SQ or neither
    if question_response_id == 4700612:
        apply_activist_codes(van_id, question_response_id, date_updated_iso)
    elif question_name_id != 0:
        apply_survey_questions(van_id, question_name_id, question_response_id, date_updated_iso)


def apply_survey_questions(vanid, question_name_id, question_response_id, date_updated) -> None:
    """
    This method adds an SR for a given SQ, for the input VAN ID, using the VAN Canvass Responses Endpoint

    :param vanid: str, MyC VAN ID input (from output of find or create function)
    :param question_name_id: int, Survery Question ID
    :param question_response_id: int, Survey Response ID
    :param date_updated: str, date record updated in ISO 8601 format
    :return: none
    """
    # NGPVAN Endpoint For Canvass Responses
    VAN_SQ_ENDPOINT_URL = f"https://api.securevan.com/v4/people/{vanid}/canvassResponses"

    # Data for SQ update. This can create multiple SRs for the same SQ : ("skipMatching": True).
    # This is needed, as there are multiple values that someone can choose that map to the same SQ,
    # in the TX Dems Vol signup form
    payload = {
        "canvassContext": {
            "inputTypeId": 11,
            "contactTypeId": 75,
            "dateCanvassed": date_updated,
            "skipMatching": True
        },
        "responses": [
            {
                "action": "Apply",
                "type": "SurveyResponse",
                "surveyQuestionId": question_name_id,
                "surveyResponseId": question_response_id
            }
        ]
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": os.getenv("VAN_AUTH")
    }

    # Sends the data to the Canvass Results Endpoint
    response = requests.post(VAN_SQ_ENDPOINT_URL, json=payload, headers=headers)


def apply_activist_codes(vanid, activist_code_id, date_updated) -> None:
    """
    This method adds an AC for the input VAN ID, using the VAN Canvass Responses Endpoint

    :param vanid: str, MyC VAN ID input (from output of find or create function)
    :param activist_code_id: int, Activist Code ID
    :param date_updated: str, date record updated in ISO 8601 format
    :return: none
    """

    # NGPVAN Endpoint For Canvass Responses
    VAN_AC_ENDPOINT_URL = f"https://api.securevan.com/v4/people/{vanid}/canvassResponses"

    # Data for AC Application
    payload = {
        "canvassContext": {
            "inputTypeId": 11,
            "contactTypeId": 75,
            "dateCanvassed": date_updated
        },
        "responses": [
            {
                "action": "Apply",
                "type": "ActivistCode",
                "activistCodeId": activist_code_id
            }
        ]
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": os.getenv("VAN_AUTH")
    }

    # Sends the data to the Canvass Results Endpoint
    response = requests.post(VAN_AC_ENDPOINT_URL, json=payload, headers=headers)


def actionkit_to_van():
    """
    This method queries the ActionKit endpoint with the intended SQL, and sends the data
     to the methods that ping the VAN API, for the Find or Create endpoint and Canvass Responses endpoint in VAN
    :return: list of records updated, for stac labs ticker
    """
    # Actionkit Endpoint for SQL query
    AK_ENDPOINT_URL = '/rest/v1/report/run/sql/'

    # Action Kit SQL Query--  Can change this as needed!
    query = "SELECT DISTINCT m.user_id, first_name, middle_name, last_name, city, state, zip, email, " \
            "normalized_phone, subscription_status as email_subscription_status, name, " \
            "value, m.id as core_action_id, " \
            "m.created_at, m.updated_at, page_id FROM (SELECT * FROM (SELECT a.user_id, " \
            "u.first_name, u.middle_name, u.last_name, u.city, u.state, u.zip, u.email, " \
            "u.subscription_status, a.id, a.created_at, a.updated_at, f.name, f.value, a.page_id " \
            "FROM core_action as a LEFT JOIN core_actionfield as f ON a.id = f.parent_id " \
            "LEFT JOIN core_user as u ON a.user_id = u.id WHERE page_id = 346) as l " \
            "UNION ALL SELECT * FROM (SELECT a.user_id, u.first_name, u.middle_name, u.last_name, u.city, " \
            "u.state, u.zip, u.email, u.subscription_status, a.id, a.created_at, a.updated_at, f.name, f.value, " \
            "a.page_id FROM core_action as a LEFT JOIN core_userfield as f ON a.id = f.action_id " \
            "LEFT JOIN core_user as u ON a.user_id = u.id WHERE page_id = 346) as l WHERE name " \
            "IN ('gender','sms_subscriber') AND value IN('Prefer not to say','Man','Yes','Woman','Non-Binary'))" \
            " as m LEFT JOIN core_phone as p ON m.user_id = p.user_id AND (DATE(m.updated_at) = DATE(p.updated_at) OR DATE(m.created_at) = DATE(p.created_at))" \
            "WHERE DATE(m.created_at) = DATE_SUB(DATE(CONVERT_TZ(CURRENT_TIMESTAMP(),'+00:00','-05:00')), INTERVAL 1 DAY) OR " \
            "DATE(m.updated_at) = DATE_SUB(DATE(CONVERT_TZ(CURRENT_TIMESTAMP(),'+00:00','-05:00')), INTERVAL 1 DAY)"

    # Setting the data variable
    data = {"query": query}

    # Sending the SQL query to Action Kit, and getting back the response data
    response = requests.post(
        f'https://{os.getenv("AK_USERNAME")}:{os.getenv("AK_PASSWORD")}@{os.getenv("AK_DOMAIN")}{AK_ENDPOINT_URL}',
        data=data)
    r = response.json()

    # A for-loop that grabs data from the incoming rows, and sends them to MyC's Find or Create function (and additionally the AC/SQ application methods)
    # This goes row by row for incoming data.
    # Since the script is to pull the day before's data, "updated_date" is used to make sure the record reflects the most recent canvass
    # Column names in order of input (from SQL generated table) are:
    # first_name, middle_name, last_name, city, state, zip, email, normalized_phone, email_subscription_status, name, value, updated_at

    for i in r:
        find_and_create_and_apply(i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[14])

    # Remove at some point (for testing counts only)
    # print(r)

    # for ticker- unique record updated counts
    unique_record_counts = len(set([i[0] for i in r]))

    # for ticker- activist code updated counts
    activist_code_update_counts = len([i[11] for i in r if i[11] == 'Lawyer/Legal Professional'])

    # for ticker- survey question updated counts
    survey_question_update_counts = len([i[11] for i in r if i[10] in ['identity', 'languages', 'volunteer_opportunities', 'race'] and i[11] not in ['Lawyer/Legal Professional']])

    return [unique_record_counts, activist_code_update_counts, survey_question_update_counts]


def phone_validation(phone) -> str:
    """
    This method validates phone numbers using VAN's API, and if number is not valid the phone variable
    is assigned an empty string

    :param phone: str, phone number from ActionKit
    :return: str, empty string if phone number was not valid, or returns the vaild phone number
    """
    VAN_PHONE_ENDPOINT = "https://api.securevan.com/v4/people/findByPhone"
    payload = {"phoneNumber": f"{phone}"}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": os.getenv("VAN_AUTH")
    }
    response = requests.post(VAN_PHONE_ENDPOINT, json=payload, headers=headers)
    if response.status_code == 400:
        phone = ""
    return phone


def email_validation(email) -> str:
    """
    Email validation method. Does not guess what the email is by removing not allowed characters;
    if email is not valid, it is removed.

    :param email: str, input email
    :return: email
    """
    # Set regex pattern
    html_pattern = re.compile("[\<\>]+|&#")

    # Handle NoneType
    email = email or ""

    # Html character exclusion and "/" character exclusion
    if html_pattern.search(email):
        email = ""
    elif "/" in email:
        email = ""

    # Using email validator package to validate email
    try:
        # Check that the email address is valid.
        validation = validate_email(email)
        # Return the normalized form of the email address
        email = validation.email
    except EmailNotValidError:
        email = ""
    finally:
        return email


def name_and_place_validation(van_input) -> str:
    """
    General validation for input fields, removing numbers, html tags and html special characters

    :param van_input: str, the input field (i.e. city, first_name, last_name)
    :return: str, the van input is returned after validation
    """
    # Set regex pattern
    html_pattern = re.compile("[\<\>]+|&#|[0-9]")

    # Handle NoneType
    van_input = van_input or ""

    # Remove html patterns and capitalize first letter of every word
    van_input = html_pattern.sub("", van_input).title()

    return van_input


def zip_validation(zip_input) -> str:
    """
    Formats input zip to zip code 5

    :param zip_input: str, zipcode value from Actionkit
    :return: str, zip_input
    """
    # Set regex pattern
    html_pattern = re.compile("[^0-9]")

    # Handle NoneType
    zip_input = zip_input or ""

    # Remove anything that is not a number
    zip_input = html_pattern.sub("", zip_input)

    # limit to zip5
    zip_input = zip_input[:5]
    if len(zip_input) != 5:
        zip_input = ""

    return zip_input


if __name__ == '__main__':
    # Send data to TDP MyC VAN; return counts for ticker
    counts = actionkit_to_van()

    # ticker object
    ticker = TickerRequest()

    # add data to ticker
    ticker.add_data('TX', 'DNC Portal', 'find_or_create_person', 'records updated', counts[0])
    ticker.add_data('TX', 'DNC Portal', 'add_or_remove_ac', 'records updated', counts[1])
    ticker.add_data('TX', 'DNC Portal', 'add_survey_question', 'records updated', counts[2])

    # Remove at some point (for testing counts only)
    # print(ticker.data)

    # Send data to ticker
    ticker.send_to_ticker()
