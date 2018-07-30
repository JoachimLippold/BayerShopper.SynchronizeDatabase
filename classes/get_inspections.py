#!/usr/bin/env python
# -*- coding: utf-8 -*-

u"""
Ermittelt die markierten Apotheken in Salesforce und baut eine entsprechende
Datenstruktur auf.
"""

from __future__ import print_function

import os, sys, datetime, exceptions, collections, copy
import pprint

from swdb import SWDB

class GetInspections(object):

    """const"""
    SOQL_DATEFORMAT = '%Y-%m-%dT%H:%M:%SZ'

    """private"""
    __app, __tour_date, __from_date, __to_date = (None,)*4
    __one_day = datetime.timedelta(days=1)

    """public"""
    pharmacies = (None,)*1

    def __init__(self, app, tour_date):
        if not hasattr(app, 'salesforce'):
            raise AttributeError(u'Object \'app\' has no attribute \'salesforce\'')

        self.__app = app
        self.__tour_date = datetime.datetime.strptime(tour_date, '%d.%m.%Y')
        self.__from_date = self.__tour_date - self.__one_day
        self.__to_date = self.__tour_date + self.__one_day

        self.__app.logger.debug("tour_date = {0}, __tour_date = {1}, __from_date = {2}, __to_date = {3}" . 
                format(tour_date, self.__tour_date, self.__from_date, self.__to_date))


    def getInspections(self):
        u"""Gets inspections from salesforce and creates data structure. Returns list of objects"""
        result = []
        query = u"""
            SELECT Shopper_Contract__c, Id, Name,
                    Shopper_Contract__r.Account_Information__c, Shopper_Contract__r.Status__c,
                    Shopper_Contract__r.Shelf_Details__c, Shopper_Contract__r.Shopper_Termination__c,
                    Shopper_Contract__r.Shopper_Termination_Reason__c, Shopper_Contract__r.Contact__c,
                    Shopper_Contract__r.IsDeleted, Shopper_Contract__r.Active__c,
                    Shopper_Contract__r.Shelf_Length__c, Shopper_Contract__r.Shelf_Width__c
                FROM Shopper_Inspection__c
                WHERE CreatedDate > {:1} AND CreatedDate < {:2} AND Status__c = 'Open'
        """ . format(self.__from_date.strftime(self.SOQL_DATEFORMAT), self.__to_date.strftime(self.SOQL_DATEFORMAT))

        records = self.__app.salesforce.query_all(query)

        for record in records['records']:
            self.flattenRecord(record)
            result.append(record)

        return result


    def flattenRecord(self, record):
        u"""Flattens record and removes unwanted entries"""
        del record[u'attributes']
        pharmacy = record.pop(u'Shopper_Contract__r')
        del pharmacy[u'attributes']
        record.update(pharmacy)
    
        self.splitAccountInformation(record)


    def printRecord(self, record, depth=0, maxDepth=5):
        for key, value in record.items():
            if key <> u'attributes':
                if depth > maxDepth:
                    return
                elif isinstance(value, collections.OrderedDict):
                    self.printRecord(value, depth=depth+1)
                else:
                    print(u"{0:29s} | {1:17s} | {2:}" . format(key, type(value), value))


    def splitAccountInformation(self, record):
        try:
            parts = record[u'Account_Information__c'].split('<br>')
        except AttributeError, msg:
            self.__app.logger.debug(msg)
            parts = record[u'Account_Information__c']

        if isinstance(parts, list) and len(parts) > 7:
            d = collections.OrderedDict()
            d[u'pharmacy'] = parts[0]
            d[u'sap_id'] = parts[1]
            d[u'strasse'] = parts[2]
            d[u'plz'], void, d[u'ort'] = parts[3].partition(u' ')
            d[u'state'], void, d[u'country'] = parts[4].partition(u' ')
            d[u'extra'] = parts[5]
            void, void, d[u'email'] = parts[6].partition(u' ')
            void, void, d[u'phone'] = parts[7].partition(u' ')
            record.update(d)
            del record[u'Account_Information__c']


    def getMetadata(self):
        meta = self.__app.salesforce.Shopper_Contract__c.metadata()
        pp = pprint.PrettyPrinter(indent=4)
        for record in meta['objectDescribe']:
           pp.pprint(record)


    def getDescription(self):
        descr = self.__app.salesforce.Shopper_Contract__c.describe()
        for field in descr['fields']:
            print(field)


if __name__ == '__main__':
    sys.exit("This module is not for execution")

