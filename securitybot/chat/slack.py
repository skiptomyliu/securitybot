'''
A wrapper over the Slack API.
'''
__author__ = 'Alex Bertsch'
__email__ = 'abertsch@dropbox.com'

import logging
from slackclient import SlackClient
import json
import time

from securitybot.user import User
from securitybot.chat.chat import Chat, ChatException

from typing import Any, Dict, List

RATE_LIMIT_SLEEP = 10 # sleep 10 seconds when rate limit
RATE_LIMIT_TRIES = 6  # maximum 6 tries (60s) when rate limit reached

class Slack(Chat):
    '''
    A wrapper around the Slack API designed for Securitybot.
    '''
    def __init__(self, username, token, icon_url):
        # type: (str, str, str) -> None
        '''
        Constructs the Slack API object using the bot's username, a Slack
        token, and a URL to what the bot's profile pic should be.
        '''
        self._username = username
        self._icon_url = icon_url

        self._slack = SlackClient(token)
        self._validate()

    def _validate(self):
        # type: () -> None
        '''Validates Slack API connection.'''
        response = self._api_call('api.test')
        if not response['ok']:
            raise ChatException('Unable to connect to Slack API.')
        logging.info('Connection to Slack API successful!')

    def _api_call(self, method, rate_limit_retry=True, **kwargs):
        # type: (str, **Any) -> Dict[str, Any]
        '''
        Performs a _validated_ Slack API call. After performing a normal API
        call using SlackClient, validate that it has not been rate limited.  If it has,
        retry every RATE_LIMIT_SLEEP until RATE_LIMIT_TRIES reached.
        Validate that the call returned 'ok'. If not, log and error.

        Args:
            method (str): The API endpoint to call.
            **kwargs: Any arguments to pass on to the request.
        Returns:
            (dict): Parsed JSON from the response.
        '''
        cur_try = 0
        while True:
            response = self._slack.api_call(method, **kwargs)
            if cur_try > RATE_LIMIT_TRIES:
                raise ChatException('Slack rate limit max tries reached.')

            if response.get('error', '').lower() == 'ratelimited' and rate_limit_retry:
                logging.debug('Rate limiting reached.  Sleeping {}.'.format(RATE_LIMIT_SLEEP))
                cur_try += 1
                time.sleep(RATE_LIMIT_SLEEP)
            else:
                break

        if not ('ok' in response and response['ok']):
            if kwargs:
                logging.error('Bad Slack API request on {} with {}'.format(method, kwargs))
            else:
                logging.error('Bad Slack API request on {}'.format(method))
        return response

    def connect(self):
        # type: () -> None
        '''Connects to the chat system.'''
        logging.info('Attempting to start Slack RTM session.')
        if self._slack.rtm_connect():
            logging.info('Slack RTM connection successful.')
        else:
            raise ChatException('Unable to start Slack RTM session')

    def get_users(self):
        # type: () -> List[Dict[str, Any]]
        '''
        Returns a list of all users in the chat system.

        Returns:
            A list of dictionaries, each dictionary representing a user.
            The rest of the bot expects the following minimal format:
            {
                "name": The username of a user,
                "id": A user's unique ID in the chat system,
                "profile": A dictionary representing a user with at least:
                    {
                        "first_name": A user's first name
                    }
            }
        '''
        members = []
        next_cursor = None
        while True:
            response = self._api_call('users.list', cursor=next_cursor)
            active_members = [m for m in response['members'] if not m.get('deleted')]
            members.extend(active_members)
            logging.debug('Fetched {} members'.format(len(members)))

            metadata = response.get('response_metadata')
            if not metadata or not metadata.get('next_cursor'):
                break

        return members

    def get_messages(self):
        # type () -> List[Dict[str, Any]]
        '''
        Gets a list of all new messages received by the bot in direct
        messaging channels. That is, this function ignores all messages
        posted in group chats as the bot never interacts with those.

        Each message should have the following format, minimally:
        {
            "user": The unique ID of the user who sent a message.
            "text": The text of the received message.
        }
        '''
        events = self._slack.rtm_read()
        messages = [e for e in events if e['type'] == 'message']
        return [m for m in messages if 'user' in m and m['channel'].startswith('D')]

    def send_message(self, channel, message):
        # type: (Any, str) -> None
        '''
        Sends some message to a desired channel.
        As channels are possibly chat-system specific, this function has a horrible
        type signature.
        '''
        self._api_call('chat.postMessage', channel=channel,
                                           text=message,
                                           username=self._username,
                                           as_user=False,
                                           icon_url=self._icon_url)

    def message_user(self, user, message):
        # type: (User, str) -> None
        '''
        Sends some message to a desired user, using a User object and a string message.
        '''
        channel = self._api_call('im.open', user=user['id'])['channel']['id']
        self.send_message(channel, message)
