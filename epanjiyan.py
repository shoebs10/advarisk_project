import concurrent.futures
from time import sleep
import requests
import json
from lxml import html
from datetime import date, datetime
import pandas as pd
from urllib.parse import urlparse, quote, unquote, urlencode
import re
import os
from PIL import ImageFile, Image
import easyocr
from io import BytesIO, StringIO
import pymongo

list_df = []
cc = 'in'
dom = 'in'
website = 'epanjiyan'
base_path = os.path.join(os.getcwd(), 'output', website)
website_path = os.path.join(base_path, str(date.today()))
[os.makedirs(directory, exist_ok=True) for directory in [base_path, website_path]]


def construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation,
                        district_id='', tehsil_id='', sro_id='', doc_id='', page='', doc_num=''):

    data = (
        f"ctl00%24ScriptManager1=ctl00%24upContent%7C{quote(script_manager)}&"
        f"ScriptManager1_HiddenField=&"
        f"ctl00%24ContentPlaceHolder1%24a={radio_value}&"
        f"ctl00%24ContentPlaceHolder1%24ddlDistrict={district_id}&"
        f"ctl00%24ContentPlaceHolder1%24ddlTehsil={tehsil_id}&"
        f"ctl00%24ContentPlaceHolder1%24ddlSRO={sro_id}&"
        f"ctl00%24ContentPlaceHolder1%24ddldocument={doc_id}&"
        f"ctl00%24ContentPlaceHolder1%24txtexcutent=&"
        f"ctl00%24ContentPlaceHolder1%24txtclaiment={doc_num}&"
        f"ctl00%24ContentPlaceHolder1%24txtexecutentadd=&"
        f"ctl00%24ContentPlaceHolder1%24txtprprtyadd=&"
        f"ctl00%24ContentPlaceHolder1%24txtimgcode=&"
        f"ctl00%24hdnCSRF=&"
        f"__EVENTTARGET={quote(script_manager)}&"
        f"__EVENTARGUMENT=&"
        f"__LASTFOCUS=&"
        f"__VIEWSTATE={quote(view_state)}&"
        f"__VIEWSTATEGENERATOR={view_state_generator}&"
        f"__SCROLLPOSITIONX=0&"
        f"__SCROLLPOSITIONY=0&"
        f"__EVENTVALIDATION={quote(event_validation)}&"
        f"__VIEWSTATEENCRYPTED=&"
        f"__ASYNCPOST=true"
    )

    if page:
        data = data.replace("__EVENTARGUMENT=&", f"__EVENTARGUMENT=Page%24{page}&")
        data += '&ctl00%24ContentPlaceHolder1%24ddlcolony=-Select-'

    return data


def captcha_function(captcha_url, session):
    req_cap = send_request(captcha_url, '', session)
    if req_cap is None:
        return None
    if req_cap.status_code != 200:
        return None

    captcha_text = extract_text_from_image(req_cap)
    return captcha_text


def insert_dataframe_to_mongo(merged_table, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    data_dict = merged_table.to_dict(orient='records')
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    try:
        collection.insert_many(data_dict)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")


def save_html_response(response, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(response)


def extract_value(xml, xpath_expression):
    try:
        values = xml.xpath(xpath_expression)
        value = ' '.join([s.strip() for s in values])
    except:
        value = ''
    return value


def extract_value_json(json_data, json_path):
    try:
        value = extract_json_path(json_data, json_path)
    except (json.JSONDecodeError, KeyError):
        value = ''
    return value


def extract_json_path(data, json_path):
    try:
        keys = json_path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            elif isinstance(value, list):
                try:
                    index = int(key)
                    if 0 <= index < len(value):
                        value = value[index]
                    else:
                        value = ''
                except ValueError:
                    value = ''
            else:
                value = ''
                break
        if isinstance(value, (str, int, float)):
            value = str(value)
        else:
            value = ''
    except (KeyError, IndexError, TypeError):
        value = ''
    return value


def send_request(hit_url, data, session):
    retry_count = 0
    max_retries = 3
    r = None
    while retry_count < max_retries:
        try:
            with return_response_sa(hit_url, data, session) as r:
                if r.status_code == 404:
                    break
                r.raise_for_status()
            break
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            retry_count += 1
            print(f"Request failed: {e}")
            if str("InvalidChunkLength").lower() in str(e).lower():
                print("Skipping request due to InvalidChunkLength error.")
                return None
            if retry_count == max_retries:
                print("Max retries reached. Exiting...")
            else:
                print(f"Retrying ({retry_count}/{max_retries})...")
                sleep(3)
    return r


def make_request(hit_url, data, session):
    req = send_request(hit_url, data, session)
    if req is None:
        return None, None
    if req.status_code != 200:
        return None, None

    res = req.text
    s = html.fromstring(res)
    return res, s


def return_response_sa(hit_url, data, session):
    try:
        if data:
            res = session.post(hit_url, data=data, timeout=120)
        else:
            res = session.get(hit_url, timeout=120)
        return res
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def extract_re_info(js_code):
    if js_code:
        if '\\' in js_code:
            match = re.search(r"__doPostBack\(\\'(.*?)\\\',", js_code)
        else:
            match = re.search(r"__doPostBack\('([^']+)',", js_code)
        if match:
            first_argument = match.group(1)
            return first_argument
        else:
            return ''
    else:
        return ''


def extract_text_from_image(response):
    image = Image.open(BytesIO(response.content))
    reader = easyocr.Reader(['en'])
    output = reader.readtext(image)
    extracted_text = " ".join([text[1] for text in output])
    return extracted_text


def get_data_between(s, start_marker, end_marker):
    start_index = s.find(start_marker)
    if start_index != -1:
        end_index = s.find(end_marker, start_index + len(start_marker))
        if end_index != -1:
            return s[start_index + len(start_marker):end_index]


def extract_view_state_data(s):
    view_state = get_data_between(s, '__VIEWSTATE|', '|')
    view_state_generator = get_data_between(s, '__VIEWSTATEGENERATOR|', '|')
    event_validation = get_data_between(s, '__EVENTVALIDATION|', '|')
    return view_state, view_state_generator, event_validation


def get_url(sx, location_type, district, tehsil, sro, document_type, document_no):
    session = requests.Session()

    session.headers.update({
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Referer': 'https://epanjiyan.rajasthan.gov.in/e-search-page.aspx',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    })

    hit_url = 'https://epanjiyan.rajasthan.gov.in/e-search-page.aspx'
    base_url = urlparse(hit_url)._replace(path='', query='', fragment='').geturl()

    #home page
    data = ''
    res, s = make_request(hit_url, data, session)
    if res is None:
        return None

    script_name = extract_value(s, f'//input[@type="radio" and contains(@value,"{location_type}")]/@name')[:-1]
    radio_value = extract_value(s, f'//input[@type="radio" and contains(@value,"{location_type}")]/@value')
    script_manager = f'{script_name}{radio_value}'

    view_state = extract_value(s, '//input[@id="__VIEWSTATE"]/@value')
    view_state_generator = extract_value(s, '//input[@id="__VIEWSTATEGENERATOR"]/@value')
    event_validation = extract_value(s, '//input[@id="__EVENTVALIDATION"]/@value')

    #location type selection
    data1 = construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation)

    res1, s1 = make_request(hit_url, data1, session)
    if res1 is None:
        return None

    script_manager = extract_re_info(extract_value(s1, f'//select[option[contains(@value,"Select District")]]/@onchange'))
    view_state, view_state_generator, event_validation = extract_view_state_data(res1)
    district_id = extract_value(s1, f'//select[contains(@name,"District")]//option[translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz") = "{district.lower()}"]/@value')

    #district selection

    data2 = construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation, district_id=district_id)

    res2, s2 = make_request(hit_url, data2, session)
    if res2 is None:
        return None

    script_manager = extract_re_info(extract_value(s2, f'//select[option[contains(@value,"Select Tehsil")]]/@onchange'))
    view_state, view_state_generator, event_validation = extract_view_state_data(res2)
    tehsil_id = extract_value(s2, f'//select[contains(@name,"Tehsil")]//option[translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz") = "{tehsil.lower()}"]/@value')

    #tehsil selection

    data3 = construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation,
                                district_id=district_id, tehsil_id=tehsil_id)

    res3, s3 = make_request(hit_url, data3, session)
    if res3 is None:
        return None

    script_manager = extract_re_info(extract_value(s3, f'//select[option[contains(@value,"Select SRO")]]/@onchange'))
    view_state, view_state_generator, event_validation = extract_view_state_data(res3)
    sro_id = extract_value(s3, f'//select[contains(@name,"SRO")]//option[translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz") = "{sro.lower()}"]/@value')

    #sro selection

    data4 = construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation,
                                district_id=district_id, tehsil_id=tehsil_id, sro_id=sro_id)

    res4, s4 = make_request(hit_url, data4, session)
    if res4 is None:
        return None

    script_manager = extract_re_info(extract_value(s4, f'//select[contains(@name,"document")]/@onchange'))
    view_state, view_state_generator, event_validation = extract_view_state_data(res4)
    doc_id = extract_value(s4, f'//select[contains(@name,"document")]//option[translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz") = "{document_type.lower()}"]/@value')

    #document type selection

    data5 = construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation,
                                district_id=district_id, tehsil_id=tehsil_id, sro_id=sro_id, doc_id=doc_id)

    res5, s5 = make_request(hit_url, data5, session)
    if res5 is None:
        return None

    script_manager = extract_value(s5, f'//input[@type="submit" and @onclick]/@name')
    view_state, view_state_generator, event_validation = extract_view_state_data(res5)

    captcha = extract_value(s5, f'//tr[contains(@id,"ImageCode")]//img/@src')
    captcha_url = f'{base_url}/{captcha}'

    captcha_text = captcha_function(captcha_url, session)

    #summary
    data6 = {
        'ctl00$ScriptManager1': f'ctl00$upContent|{script_manager}',
        'ScriptManager1_HiddenField': '',
        'ctl00$ContentPlaceHolder1$a': radio_value,
        'ctl00$ContentPlaceHolder1$ddlDistrict': district_id,
        'ctl00$ContentPlaceHolder1$ddlTehsil': tehsil_id,
        'ctl00$ContentPlaceHolder1$ddlSRO': sro_id,
        'ctl00$ContentPlaceHolder1$ddlcolony': '-Select-',
        'ctl00$ContentPlaceHolder1$ddldocument': doc_id,
        'ctl00$ContentPlaceHolder1$txtexcutent': '',
        'ctl00$ContentPlaceHolder1$txtclaiment': f'{document_no}',
        'ctl00$ContentPlaceHolder1$txtexecutentadd': '',
        'ctl00$ContentPlaceHolder1$txtprprtyadd': '',
        'ctl00$ContentPlaceHolder1$txtimgcode': captcha_text.upper(),
        'ctl00$hdnCSRF': '',
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': view_state,
        '__VIEWSTATEGENERATOR': view_state_generator,
        '__SCROLLPOSITIONX': '0',
        '__SCROLLPOSITIONY': '0',
        '__EVENTVALIDATION': event_validation,
        '__VIEWSTATEENCRYPTED': '',
        '__ASYNCPOST': 'true',
        'ctl00$ContentPlaceHolder1$btnsummary': 'View Summary',
    }
    list_table = []
    page = 1
    while True:
        print(f'Excel row={sx+1} and {page=}')

        while True:
            res6, s6 = make_request(hit_url, data6, session)
            if res6 is None:
                return None

            if page == 1 and 'no data available' in res6.lower():
                captcha_text = captcha_function(captcha_url, session)
                data6['ctl00$ContentPlaceHolder1$txtimgcode'] = captcha_text.upper()
                continue
            else:
                break

        script_manager = extract_re_info(extract_value(s6, f'(//table[contains(@id,"gridsummary")]//tr//@href)[1]'))
        view_state, view_state_generator, event_validation = extract_view_state_data(res6)

        table_rows = s6.xpath('''(//table[contains(@id,"gridsummary")]//tr[@class='tablestyle'])[1]/following-sibling::tr[@class='tablestyle']/preceding-sibling::tr''')
        if table_rows:
            table_html = ''.join(html.tostring(row, pretty_print=True).decode() for row in table_rows)
            full_table_html = f'<table>{table_html}</table>'
            table_df = pd.read_html(StringIO(full_table_html))[0]
            list_table.append(table_df)
            page += 1

            data6 = construct_form_data(script_manager, radio_value, view_state, view_state_generator, event_validation,
                                        district_id=district_id, tehsil_id=tehsil_id, sro_id=sro_id, doc_id=doc_id,
                                        page=str(page), doc_num=str(document_no))

            if page == 4:
                break
        else:
            break

    if list_table:
        merged_table = pd.concat(list_table, ignore_index=True)
        merged_table['location_type'] = radio_value
        merged_table['district_name'] = district
        merged_table['district_code'] = district_id
        merged_table['tehsil_name'] = tehsil
        merged_table['tehsil_code'] = tehsil_id
        merged_table['sro_name'] = sro
        merged_table['sro_code'] = sro_id
        merged_table['colony_name'] = '-'
        merged_table['colony_code'] = '-'
        merged_table['document_type'] = document_type
        merged_table['document_number'] = document_no

        columns = ['location_type', 'district_name', 'district_code', 'tehsil_name', 'tehsil_code',
                   'sro_name', 'sro_code', 'colony_name', 'colony_code', 'document_type', 'document_number'] + \
                  [col for col in merged_table.columns if col not in ['location_type', 'district_name', 'district_code',
                                                                      'tehsil_name', 'tehsil_code', 'sro_name', 'sro_code',
                                                                      'colony_name', 'colony_code', 'document_type',
                                                                      'document_number']]

        merged_table = merged_table[columns]
        session.close()
        return merged_table
    else:
        print('no df')
        session.close()
        return pd.DataFrame()


def thread(fcb):
    print(f'Total PL urls {len(fcb)}')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for idx, location_type, district, tehsil, sro, document_type, document_no in zip(fcb.index, fcb['location_type'], fcb['district'], fcb['tehsil'], fcb['sro'], fcb['document_type'], fcb['document_no']):
            future = executor.submit(get_url, idx, location_type, district, tehsil, sro, document_type, document_no)
            futures.append(future)

        concurrent.futures.wait(futures)
        sleep(1)


def main(use_thread=False):
    df = pd.read_excel('input.xlsx', sheet_name=website)

    if not df.empty:
        if use_thread:
            thread(df)
        else:
            for index, row in df.iterrows():
                dfs = get_url(index, row['location_type'], row['district'], row['tehsil'], row['sro'], row['document_type'], row['document_no'])
                list_df.append(dfs)

            merged_df = pd.concat(list_df, ignore_index=True)
            # insert_dataframe_to_mongo(merged_df, "advarisk", "epanjiyan")
            merged_df.to_excel(f"{website_path}/{website}_table_{datetime.today().strftime('%m%d%Y')}.xlsx",
                                  index=False, engine='openpyxl')


if __name__ == '__main__':
    main()

print('Done')

