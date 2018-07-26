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
        self.__app.logger.debug("SELECT id FROM apo_masterdata WHERE byr_salesforce_id = '{:s}'" . format(id))

        with self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute(query, { u'salesforce_id': id })
            except Exception as msg:
                self.__app.logger.error(msg)
                sys.exit(u'Exception occured: {}' . format(msg))

        return cur.rowcount > 0


    def getActivePharmacies(self):
        u"""Gets all active pharmacies

            @return pharmacies[]
        """
        res = []
        query = u"""SELECT o.id, o.name, o.strasse, o.plz, o.ort, o.bundesland, o.email, 
                o.telefon1, o.outletart, am.byr_salesforce_id, am.byr_name, am.byr_status,
                am.byr_shelf_details, am.byr_contact_c, am.byr_is_deleted, am.byr_active,
                am.fwr_height, am.fwr_width, am.byr_shopper_termination, 
                am.byr_shopper_termination_reason, o.create_time
            FROM apo_masterdata am LEFT JOIN outlet o ON o.id = am.id
            WHERE o.aktiv
            ORDER BY o.ort, o.name"""

        with self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute(query, {})
                res = cur.fetchall()
            except Exception as msg:
                self.__app.logger.error(msg)

        return res


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

        with self.__app.postgresql.cursor() as cur:
            try:
                cur.execute(query, { u'active': active, u'id': id })
            except Exception, msg:
                self.__app.logger.error(msg)
                raise msg


    def setAllOutletsInactive(self):
        u"""
            void __setAllOutletsInactive()

            Setzt alle Outlets, die einen Eintrag in apo_masterdata haben oder als Outletart 
            das Markmal 'apotheke' in der Datenbank auf inaktiv. 
            Gehört zur Vorbereitung des Abgleichs.

            @throws Exception
        """
        query = u"""UPDATE outlet SET aktiv = false WHERE id IN (SELECT id FROM apo_masterdata) OR outletart = 'apotheke'"""

        with self.__app.postgresql.cursor() as cur:
            try:
                cur.execute(query, {})
            except Exception, msg:
                self.__app.logger.error(msg)
                raise msg

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

        with self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute(query, record)
            except Exception as msg:
                self.__app.logger.error(msg)
                raise msg

            res = cur.fetchone()['id']
        
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

        with self.__app.postgresql.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute(query, record)
            except Exception as msg:
                self.__app.logger.error(msg)
                raise msg

            res = cur.fetchone()['id']

        cur.close()
        return res


if __name__ == '__main__':
    sys.exit("This module is not for execution.")