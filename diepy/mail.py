import logging
from os.path import basename

from email.utils import formatdate

from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import smtplib
import mimetypes
import types

logger = logging.getLogger(__name__)

def email_file(email_from, email_to, email_cc, email_subject, email_body, filepath):
    if type(email_to) is not types.ListType and email_to != None:
        email_to = [email_to]
    if type(email_cc) is not types.ListType and email_cc != None:
        email_cc = [email_cc]

    HOST = "smtp.va.gov"
    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = ", ".join(email_to)
    if len(email_cc) > 0:
        msg["CC"] = ", ".join(email_cc)
    msg["Subject"] = email_subject
    msg['Date'] = formatdate(localtime=True)

    body = MIMEText(email_body)
    msg.attach(body)

    # attach a file

    msg.attach(_generate_attachment(filepath))

    server = smtplib.SMTP(HOST)

    try:
        failed = server.sendmail(email_from, email_to + email_cc, msg.as_string())
        print failed
    except Exception, e:
        logger.error("Unable to send email. Error: %s" % str(e))
    finally:
        server.close()

def _generate_attachment(filepath):
    ctype, encoding = mimetypes.guess_type(filepath)
    if ctype is None or encoding is not None:
        # No guess could be made, or the file is encoded (compressed), so
        # use a generic bag-of-bits type.
        ctype = 'application/octet-stream'
    maintype, subtype = ctype.split('/', 1)
    if maintype == 'text':
        fp = open(filepath)
        # Note: we should handle calculating the charset
        msg = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'image':
        fp = open(filepath, 'rb')
        msg = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'audio':
        fp = open(filepath, 'rb')
        msg = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(filepath, 'rb')
        msg = MIMEBase(maintype, subtype)
        msg.set_payload(fp.read())
        fp.close()
        # Encode the payload using Base64
        encoders.encode_base64(msg)
        # Set the filename parameter
    msg.add_header('Content-Disposition', 'attachment', filename=basename(filepath))

    return msg