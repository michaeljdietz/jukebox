### Jukebox Skill for Alexa
A media player skill for Alexa that allows you to serve your music library off of your local computer

## Requirements
* Tested with Python v3.6, although v3.0 and above may work
* Apache 2.4
* WSGI Web Server Gateway Interface

## Installation
- Install Python v3.6
- Install the Python v3 Package Index (python3-pip)
- Install Virtualenv (virtualenv)
- Install Apache v2.4
- Install WSGI (mod_wsgi)

* Create a directory named jukebox and change directory to it
* Clone the repository by typing "git clone git@github.com ."
* Setup a virtual environment by typing "virtualenv -p $(which python3) .venv"
* Activate the virtual environment by typing "source .venv/bin/activate"
* Install requirements by typing "pip3 install -r requirements.txt"

- Work in Progress... More to come...

## External Libraries
This application makes use of the following libraries which can be installed by running "pip3 install -r requirements.txt":
* Flask ([GitHub repository](https://github.com/pallets/flask))
* Flask-Ask ([GitHub repository](https://github.com/johnwheeler/flask-ask))
* SQLAlchemy ([GitHub repository](https://github.com/zzzeek/sqlalchemy))
* Mutagen ([GitHub repository](https://github.com/quodlibet/mutagen))