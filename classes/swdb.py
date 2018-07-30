#!/usr/bin/env python
# -*- coding: utf-8 -*-

u"""
Stellt die Verbindung zur swdb her und stellt Methoden zum Abgleich der Daten#
zur Verfügung
"""

from __future__ import print_function

import os, sys, datetime, exceptions

import psycopg2, psycopg2.extensions, psycopg2.extras

class SWDB(object):

    u"""private"""
    __app = (None,)*1

    def __init__(self, app):
        if not hasattr(app, 'postgresql'):
            raise AttributeError(u'Object \'app\' has no attribute \'postgresql\'')

        self.__app = app


    def entryExists(self, id):
        u"""
            boolean entryExists(salesforceId)

            Liefert einen Booleanwert, ob der betreffende Eintrag mit der Salesforce-ID
            in SWDB existiert.

            @param id   - Salesforce-id der Apotheke
            @return boolean
        """
        query = u"""SELECT id FROM apo_masterdata WHERE byr_salesforce_id = %(salesforce_id)s"""

        try:
            cur = self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, { u'salesforce_id': id })
        except Exception as msg:
            self.__app.logger.error(msg)
            sys.exit(u'Exception occured: {}' . format(msg))
        finally:
            cur.close()

        return cur.rowcount > 0


    def getActivePharmacies(self):
        u"""Gets all active pharmacies

            @return pharmacies[]
        """
        res = []
        query = u"""SELECT o.id, o.name, o.strasse, o.plz, o.ort, o.email, 
                o.telefon1, o.outletart, am.byr_salesforce_id, am.byr_name, am.byr_status,
                am.byr_shelf_details, am.byr_contact_c, am.byr_is_deleted, am.byr_active,
                cm.firma1 AS citymanager, o.create_time
            FROM apo_masterdata am 
                LEFT JOIN outlet o ON o.id = am.id
                LEFT JOIN outlet_gebietsleiter og ON og.outlet = o.id
                LEFT JOIN stammdaten cm ON cm.id = og.gebietsleiter
            WHERE o.aktiv
            ORDER BY o.ort, o.name"""

        try:
            cur = self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, {})
            res = cur.fetchall()
        except Exception as msg:
            self.__app.logger.error(msg)
        finally:
            cur.close()

        return res


    def getActivePharmaciesCursor(self):
        u"""Gets all active pharmacies and returns cursor. For csv export"""
        query = u"""SELECT o.id, am.jansen_id, o.name, o.strasse, o.plz, o.ort, o.email, 
                o.telefon1, o.outletart, am.byr_salesforce_id, am.byr_status,
                am.byr_shelf_details, am.byr_contact_c, 
                cm.firma1 AS citymanager, o.create_time
            FROM apo_masterdata am 
                LEFT JOIN outlet o ON o.id = am.id
                LEFT JOIN outlet_gebietsleiter og ON og.outlet = o.id
                LEFT JOIN stammdaten cm ON cm.id = og.gebietsleiter
            WHERE o.aktiv
            ORDER BY o.ort, o.name"""

        cur = self.__app.postgresql.cursor()
        cur.execute(query, {})

        return cur


    def setOutletStatus(self, id, active):
        u"""
            void __setOutletStatus(boolean)

            Setzt den Status 'aktiv' des Outlets. Als id wird dabei die
            Salesforce-Id erwartet.

            @param text     - Salesforce-Id
            @param bool     - Status
            @throws Exception
        """
        query = u"""UPDATE outlet SET aktiv = %(active)s 
                WHERE id = (SELECT id FROM apo_masterdata WHERE byr_salesforce_id = %(id)s)"""

        try:
            cur = self.__app.postgresql.cursor()
            cur.execute(query, { u'active': active, u'id': id })
        except Exception, msg:
            self.__app.logger.error(msg)
            raise msg
        finally:
            cur.close()


    def setAllOutletsInactive(self):
        u"""
            void __setAllOutletsInactive()

            Setzt alle Outlets, die einen Eintrag in apo_masterdata haben oder als Outletart 
            das Markmal 'apotheke' in der Datenbank auf inaktiv. 
            Gehört zur Vorbereitung des Abgleichs.

            @throws Exception
        """
        query = u"""UPDATE outlet SET aktiv = false WHERE id IN (SELECT id FROM apo_masterdata) OR outletart = 'apotheke'"""

        try:
            cur = self.__app.postgresql.cursor()
            cur.execute(query, {})
        except Exception, msg:
            successful = False
            self.__app.logger.error(u"Exception: {0}: {1}" . format(type(msg), msg.args))
        finally:
            cur.close()


    def insertOutlet(self, record):
        res = None
        u"""
            integer __insertOutlet(record)

            Erzeugt einen Eintrag in der Tabelle outlet. Die Daten werden dem record-Objekt
            entnommen. Als Rückgabewert wird die ID des neuen Datensatz zurückgeliefert.

            @param record   - Daten als OrderedDict
            @return integer
            @throws Exception
        """
        query = u"""INSERT INTO outlet (name, strasse, plz, ort, bundesland, email, telefon1, outletart, aktiv)
            VALUES (%(pharmacy)s, %(strasse)s, %(plz)s, %(ort)s, (SELECT code FROM bundeslaender WHERE name = %(country)s),
                %(email)s, %(phone)s, 'apotheke', true) RETURNING id"""

        cur = self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(query, record)
            res = cur.fetchone()['id']
        except Exception as msg:
            self.__app.logger.error(msg)
            raise msg
        finally:
            cur.close()
        
        return res


    def insertApoMasterdata(self, record, id):
        res = None
        record.update({u'id': id})
        u"""
            integer __insertApoMasterdata(record)

            Erzeugt einen Eintrag in der Tabelle apo_masterdata. Die Daten werden dem record-Objekt
            entnommen. Als Rückgabewert wird die ID des neuen Datensatzes zurückgeliefert.

            @param record   - Daten als OrderedDict
            @return integer
            @throws Exception
        """
        query = u"""INSERT INTO apo_masterdata (id, byr_sap_id, byr_salesforce_id, byr_name, byr_status,
                byr_shelf_details, byr_contact_c, byr_is_deleted, byr_active, fwr_height, fwr_width, 
                byr_shopper_termination, byr_shopper_termination_reason)
            VALUES (%(id)s, %(sap_id)s, %(Shopper_Contract__c)s, %(Name)s, %(Status__c)s,
                %(Shelf_Details__c)s, %(Contact__c)s, %(IsDeleted)s, %(Active__c)s,
                %(Shelf_Length__c)s, %(Shelf_Width__c)s, %(Shopper_Termination__c)s,
                %(Shopper_Termination_Reason__c)s) RETURNING id"""

        cur = self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(query, record)
            res = cur.fetchone()['id']
        except Exception as msg:
            self.__app.logger.error(msg)
            raise msg
        finally:
            cur.close()

        return res


if __name__ == '__main__':
    sys.exit("This module is not for execution.")
