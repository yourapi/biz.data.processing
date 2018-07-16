from mongoengine import DynamicDocument

class Singleton(object):
    """Class for umltiple inheritance. Together with a mongoengine-document ensures unique document:
    if a document with these keys already exists, return it. If not, create with the supplied parameters.
    If an existing document with different parameters is supplied, the different values are ignored.
    class NewDocument(BaseDocument,Singleton):
        """
    @classmethod
    def new(cls, _silent=False, **kwargs):
        fields_unique = [t[0] for t in cls._meta['unique_indexes'][0]] if cls._meta['unique_indexes'] else [cls._meta['id_field']]
        query_args = dict(zip(fields_unique, [getattr(cls, f).to_mongo(kwargs[f]) for f in fields_unique]))
        try:
            return cls.objects(**query_args).get()
        except Exception as e:
            if e.__class__.__name__ == 'DoesNotExist':
                # The exception "DoesNotExist" is not in scope. Catch the generic exception
                # and check the name of the class.
                try:
                    item = cls.objects.create(**kwargs)
                    # The create-method creates a correct DB-entry,but leaves the input-attributes unchanged.
                    # E.g. if an attribute was specified as ain integer and a string was present, the 
                    # value is still a string.
                    # Therefore, the created object must be retrieved and returned.
                    # If something went wrong, let it raise an exception.
                    return cls.objects(**query_args).get()
                except Exception as e:
                    if _silent:
                        return None
                    else:
                        raise
    def __str__(self):
        result = []
        for field in self._fields:
            if field == 'id':
                continue
            result.append((field, getattr(self, field)))
        return ', '.join(['{}: {}'.format(*t) for t in result])

class BizBase(object):
    """Base-class for mixin to get extra functinality, like export/import."""
    _fieldnames = []
    @classmethod
    def fieldnames(cls):
        "Return the fieldnames for exporting the object."
        if not cls._fieldnames:
            result = ['class'] + cls._fields.keys()
            if issubclass(cls, DynamicDocument):
                # No other way than to get aal fields from all instances:
                fieldnames = set(result)
                for item in cls.objects:
                    for fieldname in item._data:
                        if fieldname and fieldname not in fieldnames:
                            fieldnames.add(fieldname)
                            result.append(fieldname)
            cls._fieldnames = result
        return cls._fieldnames
    def row(self):
        result = {}
        for fieldname in self.fieldnames():
            try:
                value = getattr(self, fieldname)
            except:
                value = None
            if value is not None:
                try:
                    result[fieldname] = unicode(value)
                except Exception as e:
                    result[fieldname] = e.message
        result['class'] = self.__class__.__name__
        return result