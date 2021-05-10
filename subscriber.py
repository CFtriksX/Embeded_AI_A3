import paho.mqtt.client as mqtt
import time
import rdflib
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD, SOSA, TIME

class Subscriber:

    QUDT11 = Namespace('http://qudt.org/1.1/schema/qudt')
    QUDTU11 = Namespace('http://qudt.org/1.1/vocab/unit')
    CDT = Namespace('http://w3id.org/lindt/custom_datatypes')
    BASE = Namespace('http://example.org/data')

    g = Graph()
    g.bind('rdf', RDF)
    g.bind('rdfs', RDFS)
    g.bind('xsd', XSD)
    g.bind('sosa', SOSA)
    g.bind('time', TIME)
    g.bind('qudt-1-1', QUDT11)
    g.bind('qudt_unit-1-1', QUDTU11)
    g.bind('cdt', CDT)

    earthAtmo = URIRef('earthAtmosphere')
    iphone7 = URIRef('iphone7/35-207306-844818-0')
    sensor = URIRef('sensor/35-207306-844818-0/BMP282')
    sensorObs = URIRef('sensor/35-207306-844818-0/BMP282/atmosphericPressure')

    g.add( (earthAtmo, RDF.type, SOSA.FeatureOfInterest) )
    g.add( (earthAtmo, RDFS.label, Literal('Atmosphere of Earth', lang='en')) )

    g.add( (iphone7, RDF.type, SOSA.Platform) )
    g.add( (iphone7, RDFS.label, Literal('IPhone 7 - IMEI 35-207306-844818-0', lang='en')) )
    g.add( (iphone7, RDFS.comment, Literal('IPhone 7 - IMEI 35-207306-844818-0 - John Doe', lang='en')) )
    g.add( (iphone7, SOSA.host, sensor) )

    g.add( (sensor, RDF.type, SOSA.Platform) )
    g.add( (sensor, RDFS.label, Literal('Bosch Sensortec BMP282', lang='en')) )

    published = 0

    def add_obs(this, data):
        obs = URIRef('Observation/' + str(this.published))
        this.g.add( (obs, RDF.type, SOSA.Observation) )
        this.g.add( (obs, SOSA.observedProperty, this.sensorObs) )
        this.g.add( (obs, SOSA.hasFeatureOfInterest, this.earthAtmo) )
        this.g.add( (obs, SOSA.madeBySensor, this.sensor) )
        this.g.add( (obs, SOSA.hasSimpleResult, Literal(data[0], datatype=XSD['ucum'])) )
        this.g.add( (obs, SOSA.resultTime, Literal(data[1], datatype=XSD['dateTime'])) )

    def on_message(this, client, userdata, message):
        print(f"\nmessage payload: {message.payload.decode('utf-8')}")
        print(f"message topic: {message.topic}")
        print(f"message qos: {message.qos}")
        print(f"message retain flag: {message.retain}")
        this.published += 1
        this.add_obs(message.payload.decode('utf-8').split("|"))

    def __init__(this):
        print("creating new instance")
        client = mqtt.Client("P2")
        client.on_message = this.on_message

        broker_address = "test.mosquitto.org"

        try:
            client.connect(broker_address)
            client.loop_start()
            client.subscribe("teds20/group09/pressure", qos=2)

            while this.published != 10:
                pass

            client.unsubscribe("teds20/group09/pressure")
            time.sleep(4)
            client.loop_stop()
            print("\ndisconnecting from broker")
            client.disconnect()
            this.g.serialize('pressure.ttl', format='ttl')
        except Exception as e:
            print(f"connection error: {e}")
        
Subscriber()