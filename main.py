import asyncio
import aiosqlite3
import aiosmtplib

from more_itertools import chunked
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

MAIL_PARAMS = {
    'host': '127.0.0.1',
    'port': 1025,
    'mailfrom': 'test@gmail.com'
}

async def db_contacts():
    async with aiosqlite3.connect('contacts.db') as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM contacts;")
            list_contacts = await cur.fetchall()
    return list_contacts

async def sendmail_async(mailt, name, **params):
    mail_params = params.get("mail_params", MAIL_PARAMS)
    message = MIMEMultipart()
    message['From'] = mail_params.get('mailfrom')
    message['To'] = mailt
    message['Subject'] = "Hello Customer!"
    body = f'''Уважаемый(ая) {name}! Спасибо, что пользуетесь нашим сервисом объявлений.'''
    message.attach(MIMEText(body, 'plain'))
    host = mail_params.get('host', 'localhost')
    port = mail_params.get('port')
    smtp = aiosmtplib.SMTP(hostname=host, port=port)
    await smtp.connect()
    result = await smtp.send_message(message)
    await smtp.quit()
    return result

async def main():
    contacts = await db_contacts()
    for contacts_chunk in chunked(contacts, 10):
        sendmail_coroutines = [
            sendmail_async(mailt=mailt, name=f'{first_name} {last_name}')
            for _, first_name, last_name, mailt, *other in contacts_chunk
        ]
        await asyncio.gather(*sendmail_coroutines)

if __name__ == '__main__':
    asyncio.run(main())