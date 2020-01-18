#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 18:08:27 2020

@author: adriangallego
"""

import cherrypy
from paste.translogger import TransLogger
from app import create_app

def run_server(app):

    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 8100,
        'server.socket_host': '127.0.0.1'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    # Init spark context and load libraries
    #sc = init_spark_context()
    #dataset_path = os.path.join(".",'anime')
    app = create_app()

    # start web server
    run_server(app)