# -*- coding: utf-8 -*-
import logging
import sys
import settings
from datetime import datetime
from irc.bot import SingleServerIRCBot
from elasticsearch import Elasticsearch
"""Main module."""


def _get_logger():
    logger_name = 'twitch_ingest'
    logger_level = logging.DEBUG
    log_line_format = '%(asctime)s | %(name)s - %(levelname)s : %(message)s'
    log_line_date_format = '%Y-%m-%dT%H:%M:%SZ'
    logger_ = logging.getLogger(logger_name)
    logger_.setLevel(logger_level)
    logging_handler = logging.StreamHandler(stream=sys.stdout)
    logging_handler.setLevel(logger_level)
    logging_formatter = logging.Formatter(log_line_format, datefmt=log_line_date_format)
    logging_handler.setFormatter(logging_formatter)
    logger_.addHandler(logging_handler)
    return logger_

logger = _get_logger()


class TwitchIngester(SingleServerIRCBot):

    VERSION = '1.0.0'

    def __init__(self, host, port, nickname, password, channel):
        logger.debug('TCIBot.__init__ (VERSION = %r)', self.VERSION)
        SingleServerIRCBot.__init__(self, [(host, port, password)], nickname, nickname)
        self.channel = channel
        self.viewers = []
        self.es = Elasticsearch("http://{}:{}".format(settings.ELK_HOST, settings.ELK_SEARCH_PORT))

    def on_welcome(self, connection, event):
        logger.debug('TCIBot.on_welcome')
        for channel in self.channel:
            connection.join(channel)

    def on_join(self, connection, event):
        logger.debug('TCIBot.on_join')
        nickname = self._parse_nickname_from_twitch_user_id(event.source)
        self.viewers.append(nickname)

    def on_pubmsg(self, connection, event):
        logger.debug('TCIBot.on_pubmsg')
        logger.debug('message = %r', event.arguments[0])
        self.es.index(
            index=event.target[1:],
            doc_type="chat",
            body={
                "username": self._parse_nickname_from_twitch_user_id(event.source),
                "timestamp": datetime.now(),
                "message": event.arguments[0]
            }
        )

    @staticmethod
    def _parse_nickname_from_twitch_user_id(user_id):
        # nickname!username@nickname.tmi.twitch.tv
        return user_id.split('!', 1)[0]


def main():
    my_bot = TwitchIngester(settings.HOST, settings.PORT, settings.USERNAME, settings.PASSWORD, settings.CHANNELS)
    my_bot.start()


if __name__ == '__main__':
    main()
