from setuptools import setup

setup(
    name='biz.data.processing',
    version='0.1',
    packages=['biz', 'biz.db', 'biz.db.micromongo', 'biz.app', 'biz.rdf', 'biz.i18n', 'biz.i18n.nl',
              'biz.i18n.nl.schema', 'biz.input', 'biz.types', 'biz.types.nl', 'biz.types.nl.kpn', 'biz.types.nl.kpn.ip',
              'biz.types.nl.kpn.swol_marketing', 'biz.types.nl.kpn.swol_marketing.odin', 'biz.types.nl.phone',
              'biz.types.nl.address', 'biz.types.nl.fsecure', 'biz.types.date', 'biz.object', 'biz.output',
              'biz.pandas', 'biz.pandas.io', 'biz.pandas.core', 'biz.pandas.tools', 'biz.collect', 'biz.extract',
              'biz.general', 'biz.handler', 'biz.trigger', 'biz.metadata', 'biz.metadata.cat', 'biz.pipeline',
              'biz.plumbing', 'biz.basetypes', 'biz.transform'],
    url='https://github.com/yourapi/biz.data.processing',
    license='',
    author='Bizservices',
    author_email='info@bizservices.nl',
    description='Basic types'
)
