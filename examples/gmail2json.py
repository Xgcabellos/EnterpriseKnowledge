import sys
import mailbox
import email
import quopri
import json
import time
from bs4 import BeautifulSoup
from dateutil.parser import parse

# What you'd like to search for in the subject of your mail.
# See Section 6.4.4 of http://www.faqs.org/rfcs/rfc3501.html
# for more SEARCH options.

Q = "Alaska"  # XXX


# Recycle some routines from Example 6-3 so that you arrive at the
# very same data structure you've been using throughout this chapter

def cleanContent(msg):
    # Decode message from "quoted printable" format
    msg = quopri.decodestring(msg)

    # Strip out HTML tags, if any are present.
    # Bail on unknown encodings if errors happen in BeautifulSoup.
    try:
        soup = BeautifulSoup(msg)
    except:
        return ''
    return ''.join(soup.findAll(text=True))


def jsonifyMessage(msg):
    json_msg = {'parts': []}
    for (k, v) in msg.items():
        json_msg[k] = v.decode('utf-8', 'ignore')

    # The To, Cc, and Bcc fields, if present, could have multiple items.
    # Note that not all of these fields are necessarily defined.

    for k in ['To', 'Cc', 'Bcc']:
        if not json_msg.get(k):
            continue
        json_msg[k] = json_msg[k].replace('\n', '').replace('\t', '') \
            .replace('\r', '').replace(' ', '') \
            .decode('utf-8', 'ignore').split(',')

    for part in msg.walk():
        json_part = {}
        if part.get_content_maintype() == 'multipart':
            continue

        json_part['contentType'] = part.get_content_type()
        content = part.get_payload(decode=False).decode('utf-8', 'ignore')
        json_part['content'] = cleanContent(content)

        json_msg['parts'].append(json_part)

    # Finally, convert date from asctime to milliseconds since epoch using the
    # $date descriptor so it imports "natively" as an ISODate object in MongoDB.
    then = parse(json_msg['Date'])
    millis = int(time.mktime(then.timetuple()) * 1000 + then.microsecond / 1000)
    json_msg['Date'] = {'$date': millis}

    return json_msg


# Consume a query from the user. This example illustrates searching by subject.

(status, data) = conn.search(None, '(SUBJECT "%s")' % (Q,))
ids = data[0].split()

messages = []
for i in ids:
    try:
        (status, data) = conn.fetch(i, '(RFC822)')
        messages.append(email.message_from_string(data[0][1]))
    except Exception, e:
        print
        e
        print
        'Print error fetching message %s. Skipping it.' % (i,)

print.len(messages)
jsonified_messages = [jsonifyMessage(m) for m in messages]

# Separate out the text content from each message so that it can be analyzed.

content = [p['content'] for m in jsonified_messages for p in m['parts']]

# Content can still be quite messy and contain line breaks and other quirks.

filename = os.path.join('resources/ch06-mailboxes/data',
                        GMAIL_ACCOUNT.split("@")[0] + '.gmail.json')
f = open(filename, 'w')
f.write(json.dumps(jsonified_messages))
f.close()

print >> sys.stderr, "Data written out to", f.name