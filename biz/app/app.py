from biz.app.config import Config
import sys, logging

class App(object):
    """General control of the biz-app. The app is controlled by the ini-file. The flow is
    defined by triggers."""
    def __init__(self):
        self.args = sys.argv[1:]
        filename_ini = self.args[0] if self.args else 'biz.yml'
        self.config = Config(filename=filename_ini)
        # The DB-class must be incorporated.
        #self.db = db.DB()
        self._index = 0
        errors = []
        try:
            s_level = self.config.logging.level
        except:
            errors.append('Getting config logging.level failed.')
            s_level = ''
        try:
            level = eval('logging.' + s_level)
        except:
            level = logging.DEBUG
            errors.append('Logging level invalid: {level}, set to DEBUG'.format(level=s_level))
        logging.basicConfig(filename=self.config.logging.filename,
                            format=self.config.logging.format, level=level)
        log = logging.getLogger()
        for line in errors:
            log.error(line)
    def unique_index(self):
        self._index += 1
        return self._index
    def run(self):
        "Main loop of the application. Start all processes for generating and processing triggers."
        from biz.plumbing.threading import Factory # Import module locally, because all mdules import the App-module and otherwise circular refenerces are created!
        factory = Factory(self)
        for thread in self.config.threads:
            args = thread.get('args', [])
            kwargs = dict(thread.get('kwargs', {}).items())  # The config-object doesn't support direct transformation to a dict-object, alas...
            kwargs_meta = dict(thread.get('meta', {}).items())
            factory.create(thread['class'], args, kwargs, kwargs_meta)
        factory.start()

def main():
    app = App()
    app.run()

if __name__ == '__main__':
    main()