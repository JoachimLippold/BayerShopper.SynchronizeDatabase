#!/usr/bin/env python
# -*- coding: utf-8 -*-

u"""
Synchronisiert Salesforce mit der Sit&Watch-Datenbank

Das Skript ruft die aktuell markierten Apotheken in Salesforce ab und gleicht diese mit
der Sit&Watch-Datenbank ab:

    * vorhandene Apotheken werden aktiv gesetzt
    * nicht vorhandene Apotheken werden neu angelegt und aktiv gesetzt
    * Apotheken in der SWDB, die nicht markiert wurden, werden inaktiv gesetzt
    
Die Liste der Apotheken werden zusätzlich als Excel-Datei exportiert.

@Copyright 2018 - Sit&Watch Media Group GmbH
@Author Joachim Lippold - lippold@sit-watch.de
@Date 2018-04-04
@Revision 1.0
@License MIT
"""

from __future__ import print_function

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'classes')))

import datetime
import logging
import json

from optparse import OptionParser
from ConfigParser import SafeConfigParser

from simple_salesforce import Salesforce, SalesforceLogin, SalesforceAuthenticationFailed
import requests

import psycopg2, psycopg2.extensions, psycopg2.extras

from get_inspections import GetInspections

class App(object):
    u"""
    Hauptklasse der Applikation

    Initialisierung, Auswertung der Optionen und Argumente, Verarbeitung der Daten
    """
    APPNAME = os.path.splitext(os.path.abspath(sys.argv[0]))[0]
    APPVERSION = "1.0"

    """ private """
    _instance, _session, _session_id, _sf_instance, _session_id, _sf_instance = (None,)*6
    _loggingLevels = { logging.NOTSET: "NOTSET", logging.DEBUG: "DEBUG", logging.INFO: "INFO",
            logging.WARNING: "WARNING", logging.ERROR: "ERROR", logging.CRITICAL: "CRITICAL" }

    """ public """
    config, logger, options, args, session, salesforce, postgresql = (None,)*7

    def __init__(self):
        self.initConfig()
        self.initOptionParser()
        self.initLogging()
        self.checkArguments()
        self.initSalesforce()
        self.initPostgresql()
        self.dispatch()


    def initConfig(self):
        u"""
            Konfiguration einlesen.

            Liest die Konfiguration aus einer Datei mit dem Name <SCRIPTNAME>.cfg, die sich im selben
            Verzeichnis wie das Skript.

            Die Konfigurationsdatei hat folgenden Aufbau:

            <pre>
                [salesforce]
                soapUsername = <SALESFORCE-BENUTZER>
                soapPassword = <SALESFORCE-PASSWORD>
                soapSecurityToken = <SALESFORCE-SECURITY-TOKEN>
                soapSandbox = False|True
                soapVersion = <VERSION> ; aktuell 38.0

                [logging]
                formatstring = %%(asctime)s - %%(filename)s - %%(funcName)s - %%(levelname)s - %%(message)s
            </pre>

            Der Abschnitt 'salesforce' enthält die Zugangsdaten zum Salesforce-Server von Bayer. Im Abschnitt
            [logging] wird das Format des Log-Strings definiert.
        """
        self.config = SafeConfigParser()
        self.config.readfp(open(self.APPNAME + '.cfg'))


    def initLogging(self):
        u"""
            Logging in eine Datei initialisieren.

            Log-Meldungen können mit self.logger.<LEVEL> in eine externe Datei geschrieben werden.
            Die Loglevel werden mit einem Parameter -v oder --verbose beim Aufruf des Scriptes
            angegeben. Default-Level ist 'ERROR'.

            Es stehen folgende Level in aufsteigender Sortierung zur Verfügung:
                * DEBUG
                * INFO
                * WARNING
                * ERROR
                * CRITICAL

            Ausgegeben werden dann nur Meldungen, die mindestens dem eingestellten Loglevel entsprechen.
            Wurde zum beispiel 'WARNING' gesetzt, werden nur Meldungen mit dem Level 'WARNING', 'ERROR'
            und 'CRITICAL' ausgegeben. 'DEBUG' und 'INFO' werden unterdrückt.

            Der Name der Datei ist Standardmäßig der Skript-Name mit der Endung .log
        """
        try:
            loggingLevel = next(key for key, value in self._loggingLevels.items() if value == self.options.verbose)
        except (StopIteration,):
            loggingLevel = logging.NOTSET

        logging.basicConfig(filename=self.options.logging, format=self.config.get('logging', 'formatstring'), filemode='a')
        self.logger = logging.getLogger(self.APPNAME + ".logger")
        self.logger.setLevel(loggingLevel)
        self.logger.debug("options: {0}, args: {1}" . format(self.options, self.args))


    def initOptionParser(self):
        u"""
            Option-Parser initialiseren.

            Das Skript kann mit diversen Optionen aufgerufen werden. Diese werden vom OptionParser
            verarbeitet. Aktuell sind folgende Optionen möglich:

                -v, --verbose <LOGLEVEL>
                    Loglevel: [DEBUG, INFO, WARNING, ERROR, CRITICAL]

                -l, --logging <LOGFILE>
                    Name des Logfiles. Default ist <SCRIPTNAME>.log

                -h, --help
                    Hilfetext
        """
        USAGE = "usage: %prog [options] tourdate"
        DESCRIPTION = u"""
        """
        VERSION = self.APPVERSION

        parser = OptionParser(usage=USAGE, version=VERSION, description=DESCRIPTION)
        parser.add_option("-v", "--verbose", dest="verbose", default="ERROR", 
                choices=[value for key, value in self._loggingLevels.items()],
                help="Loglevel: [" + ', '.join([value for key, value in self._loggingLevels.items()]) + ")")
        parser.add_option("-l", "--logging", dest="logging", default=self.APPNAME + ".log",
                help="Name und Pfad der Logdatei")
        parser.add_option("-q", "--quiet", dest="quiet", action="store_true", help=u"Unterdrücke Fortschrittsbalken")
        parser.add_option("-o", "--outfile", dest="outfile", 
                help=u"Zusätzliche Ausgabe der neuen Apotheken in eine Excel-Datei")

        (self.options, self.args) = parser.parse_args()


    def initSalesforce(self):
        u"""
            Initialisiert die Salesforce-Verbindung

            Öffnet eine Verbindung zum Salesforce-Server und etabliert eine entsprechende Session.
            Zugriffe auf Salesforce können dann mit app.salesforce.<OBJECT>.<METHOD>() durchgeführt werden.

            Beispiel:
                app.salesforce.Shopper_Inspection__c.update(<INSPECTION_ID>, { <KEY>: <VALUE>[, <KEY>: <VALUE>[, ...]] })
                führt ein Update auf einen Datensatz der Tabelle Shopper_Inspection__c durch.
        """
        self.session = requests.Session()
        try:
            self._session_id, self._sf_instance = SalesforceLogin(username=self.config.get('salesforce', 'soapUsername'), \
                    password=self.config.get('salesforce', 'soapPassword'),
                    sf_version=self.config.get('salesforce', 'soapVersion'),
                    sandbox=(self.config.get('salesforce', 'soapSandbox') == 'True'))
        except SalesforceAuthenticationFailed as e:
            self.logger.critical("login to salesforce failed: {:s}" . format(e.message))
            print("Login to salesforce failed: {:s}" . format(e.message))
            exit()

        self.salesforce = Salesforce(instance=self._sf_instance, session_id=self._session_id, session=self.session)

        self.logger.debug('Connection to Salesforce established')


    def initPostgresql(self):
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        self.postgresql = psycopg2.connect(database=self.config.get('postgresql', 'database'), 
                host=self.config.get('postgresql', 'host'),
                user=self.config.get('postgresql', 'user'),
                password=self.config.get('postgresql', 'password'))
        self.postgresql.commit()
        self.postgresql.set_client_encoding('UTF8')
        self.postgresql.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        self.logger.debug('Connection to PostgreSQL-Server established')


    def checkArguments(self):
        u"""
        Checkt die Argumente auf Konsistenz und Format
        """
        if len(self.args) < 1:
            self.logger.critical('Zu wenig Argumente')
            sys.exit('Zu wenig Argumente')

        try:
            date = datetime.datetime.strptime(self.args[0], '%d.%m.%Y')
        except ValueError, msg:
            msg = '\'{:s}\' ist kein gültiges Datum' . format(self.args[0])
            self.logger.critical(msg)
            sys.exit(msg)


    def dispatch(self):
        inspections = GetInspections(self, self.args[0])


    def __new__(self, *args, **kwargs):
        if not self._instance:
            self._instance = super(App, self).__new__(self, *args, **kwargs)

        return self._instance


if __name__ == '__main__':
    app = App()
