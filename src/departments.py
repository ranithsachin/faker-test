import logging

import conf.configs as config

from faker import Faker

class Department:
    """
        Class handling department entity tasks
    """

    def __init__(self)->None:
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename=config.APP_LOGING_PATH,encoding="utf-8",level=config.APP_LOGING_LEVEL)

    def generate_departments(self,num_dept: int=config.NO_OF_DEPARTMENTS)->list:
       try: 
            self.logger.info("Start generating departments")
            fake = Faker()
            departments = []

            for i in range(num_dept):
                Faker.seed(i)
                departments.append(fake.word())
                Faker.seed()

            return departments
       
       except Exception as e:
           self.logger.error(f"departments data generation error encountered")
           raise (e)
