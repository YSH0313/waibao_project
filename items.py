# -*- coding: utf-8 -*-

# Define here the models for your scraped items


class BiddingItem():
    def __init__(self):
        self.title = None
        self.pub_time = None
        self.url = None
        self.show_url = None
        self.source = None
        self.html = None
        self.caller = None
        self.winner = None
        self.winner_amount = None
        self.caller_amount = None
        self.data = None

    def dict(self):
        __dict = dict()
        for k, v in self.__dict__.items():
            if v:
                __dict[k] = v
        return __dict
