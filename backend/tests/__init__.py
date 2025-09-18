import logging
import sys

logging.basicConfig(stream=sys.stderr,
                    level=logging.INFO,
                    format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logger.info("Tests initialized!")
