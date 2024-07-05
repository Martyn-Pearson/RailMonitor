class MessageHandler:
    """ Base class for message handlers, derived classes must implement process() """
    def __init__(self):
        pass
    
    def process(self, message):
        """ Validates if the message is of interest and if so, handles it appropriately """
        if self.interested(message):
            self.handle(message)

    def interested(self, message):
        """ Method to be overridden to determine if we should handle it """
        raise NotImplementedError
    
    def handle(self, message):
        """ Method to be overridden to handle the message as appropriate """
        raise NotImplementedError
