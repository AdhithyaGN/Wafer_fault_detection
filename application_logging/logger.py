from datetime import datetime


class App_logger:
    def __init__(self):
        pass



    def log(self,filename,log_message):
        self.now=datetime.now()
        self.date=self.now.date()
        self.current_time=self.now.strftime("%H:%M:%S")
        filename.write(
            str(self.date) + "/" + str(self.current_time) +"\t\t" + log_message + "\n"
        )






