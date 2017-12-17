import mailbox
import email
import json

MBOX = 'resources/ch06-mailboxes/data/northpole.mbox'


# A routine that makes a ton of simplifying assumptions
# about converting an mbox message into a Python object
# given the nature of the northpole.mbox file in order
# to demonstrate the basic parsing of an mbox with mail
# utilities

def objectify_message(msg):
    # Map in fields from the message
    o_msg = dict([(k, v) for (k, v) in msg.items()])

    # Assume one part to the message and get its content
    # and its content type

    part = [p for p in msg.walk()][0]
    o_msg['contentType'] = part.get_content_type()
    o_msg['content'] = part.get_payload()

    return o_msg


# Create an mbox that can be iterated over and transform each of its
# messages to a convenient JSON representation

mbox = mailbox.UnixMailbox(open(MBOX, 'rb'), email.message_from_file)

messages = []

while 1:
    msg = mbox.next()

    if msg is None: break

    messages.append(objectify_message(msg))

print
json.dumps(messages, indent=1)