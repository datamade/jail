import re
import datetime
import lxml.html
import locale

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

def to_date(date_string):
    return datetime.datetime.strptime(date_string, '%m/%d/%Y').date()

def text(element) :
    return element.text_content().strip()

def parse_multiline(multiline_string):
    seen = set()
    for each in re.split('[\r\n\t\xa0]', multiline_string):
        each = each.strip(', ')
        if each and each.strip() not in seen:
            yield each
            seen.add(each)

def extract_page(response):
    page = lxml.html.fromstring(response.content)
    
    rows = ((('id', 'name', 'dob', 'race', 'sex', 'height', 'weight'),
             '//table[1]/tr[2]/td'),
            (('booked date', 'housing location',
              'visiting information', 'bail amount'),
             '//table[2]/tr[2]/td'),
            (('charges',),
             '//table[2]/tr[4]/td'),
            (('next court date', 'courthouse location'),
             '//table[3]/tr[2]/td'))
    
    return {k : text(page.xpath(path)[i])
            for keys, path in rows
            for i, k in enumerate(keys)}
  
def parse_page(response):
    inmate = extract_page(response)
    inmate['charges'] = tuple(parse_multiline(inmate['charges']))
    if len(inmate['charges']) == 1:
        inmate['charges'] += (None,)
    inmate['courthouse location'] = ' '.join(parse_multiline(inmate['courthouse location']))
    inmate['visiting information'] = ' '.join(parse_multiline(inmate['visiting information']))
    inmate['dob'] = to_date(inmate['dob'])
    inmate['booked date'] = to_date(inmate['booked date'])
    inmate['next court date'] = to_date(inmate['next court date']) if inmate['next court date'] else None

    feet, inches =  int(inmate['height'][0]), int(inmate['height'][1:3])
    inmate['height'] = feet * 12 + inches
    if inmate['bail amount'] == '*NO BOND*':
        inmate['bail status'] = inmate['bail amount']
        inmate['bail amount'] = None
    else:
        inmate['bail status'] = None
        inmate['bail amount'] = locale.atof(inmate['bail amount'])
                
    
    return inmate


