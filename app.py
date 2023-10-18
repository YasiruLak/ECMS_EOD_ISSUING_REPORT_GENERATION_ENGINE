from flask import Flask
import logging
import logging as logger
from waitress import serve
from Utils import *

# logger.basicConfig(level="DEBUG")

app = Flask(__name__)
app.logger = logging.getLogger(__name__)
app.logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
app.logger.addHandler(handler)

# Set up a file handler for ERROR level logs
error_handler = logging.FileHandler(LOG_PATH + 'error.log')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)
app.logger.addHandler(error_handler)

# set up file handler for INFO logs
info_handler = logging.FileHandler(LOG_PATH + 'info.log')
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)
app.logger.addHandler(info_handler)

# if __name__ == '__main__':
#     from Controller import *
#     logger.debug("Start Issuing Statement Generation Module Version : " + VERSION)
#     serve(app, host="0.0.0.0", port=6000, threads=5)
#     # app.run()

if __name__ == '__main__':
    from Controller import *
    app.logger.debug("Start Issuing Statement Generation Module Version")
    app.run()


