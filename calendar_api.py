import datetime
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from werkzeug.utils import cached_property


class CalendarAPI():
    CLIENT_SECRET_FILE = 'credentials.json'
    API_NAME = 'calendar'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/calendar']

    @cached_property
    def credentials(self):
        credentials = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                credentials = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.CLIENT_SECRET_FILE,
                    self.SCOPES
                )
                credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(credentials, token)
        return credentials

    @cached_property
    def service(self):
        service = build(
            self.API_NAME,
            self.API_VERSION,
            credentials=self.credentials
        )
        return service

    def get_upcoming_events(
            self,
            service,
            calendar_id='primary',
            single_events=True,
            order_by='startTime'):
        # 'Z' indicates UTC time
        now = datetime.datetime.utcnow().isoformat() + 'Z'
        events_result = self.service.events().list(
            calendarId=calendar_id,
            timeMin=now,
            singleEvents=single_events,
            orderBy=order_by).execute()
        events = events_result.get('items', [])
        return events

    def add_event(self, event, calendar_id='primary'):
        event_response = self.service.events().insert(
            calendarId=calendar_id,
            body=event).execute()
        print(event_response.get("id"))

    def delete_event(self, event_id, calendar_id='primary'):
        self.service.events().delete(
            calendarId=calendar_id,
            eventId='eventId').execute()
