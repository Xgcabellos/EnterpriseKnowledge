import re
import email
from time import asctime
import os
import sys
from dateutil.parser import parse  # pip install python_dateutil

# XXX: Download the Enron corpus to resources/ch06-mailboxes/data
# and unarchive it there.

MAILDIR = 'resources/ch06-mailboxes/data/enron_mail_20110402/' + \
          'enron_data/maildir'

# Where to write the converted mbox
MBOX = 'resources/ch06-mailboxes/data/enron.mbox'

# Create a file handle that we'll be writing into...
mbox = open(MBOX, 'w')

# Walk the directories and process any folder named 'inbox'

for (root, dirs, file_names) in os.walk(MAILDIR):

    if root.split(os.sep)[-1].lower() != 'inbox':
        continue

    # Process each message in 'inbox'

    for file_name in file_names:
        file_path = os.path.join(root, file_name)
        message_text = open(file_path).read()

        # Compute fields for the From_ line in a traditional mbox message

        _from = re.search(r"From: ([^\r]+)", message_text).groups()[0]
        _date = re.search(r"Date: ([^\r]+)", message_text).groups()[0]

        # Convert _date to the asctime representation for the From_ line

        _date = asctime(parse(_date).timetuple())

        msg = email.message_from_string(message_text)
        msg.set_unixfrom('From %s %s' % (_from, _date))

        mbox.write(msg.as_string(unixfrom=True) + "\n\n")

mbox.close()