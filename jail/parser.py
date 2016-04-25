import re
import datetime

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

def extract_page(page):
    rows = ((('id', 'name', 'dob', 'race', 'sex', 'height', 'weight'),
             '//table[1]/tr[2]/td'),
            (('booked date', 'housing location',
              'visiting information', 'bail amount'),
             '//table[2]/tr[2]/td'),
            (('charges',),
             '//table[2]/tr[4]/td'),
            (('next court date', 'court house location'),
             '//table[3]/tr[2]/td'))
    
    return {k : text(page.xpath(path)[i])
            for keys, path in rows
            for i, k in enumerate(keys)}
  
def parse_page(page):
    inmate = extract_page(page)
    inmate['charges'] = tuple(parse_multiline(inmate['charges']))
    inmate['court house location'] = ' '.join(parse_multiline(inmate['court house location']))
    inmate['visiting information'] = tuple(parse_multiline(inmate['visiting information']))
    inmate['dob'] = to_date(inmate['dob'])
    inmate['booked date'] = to_date(inmate['booked date'])
    inmate['next court date'] = to_date(inmate['next court date']) if inmate['next court date'] else None

    feet, inches =  int(inmate['height'][0]), int(inmate['height'][1:3])
    inmate['height'] = feet * 12 + inches
    
    return inmate


