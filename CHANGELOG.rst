Ver. 0.6.2 - 2011 Nov 14
============================
* A bug fix release.
* Critical bug fix in ``delete`` method when a model has no indices.
* Critical bug fix in `ManyToManyField`.
* **299 regression tests**.

Ver. 0.6.1 - 2011 Sep 10
============================
* This is a minor release which brings an improved documentation,
  better support for the :class:`stdnet.orm.JSONField` and some minor
  bug fixes.
* Test suite parsing is done using the new python ``argparse`` module since the
  ``optparse`` is now deprecated. Check :ref:`running tests <runningtests>`
  for more information.
* Started work on ``lua extensions`` and added a development test tag ``script``.
* Added ``google analytics`` to the documentation web site.
* The instance validation algorithm catches :class:`stdnet.FieldValueError`
  exceptions and stores them into the errors dictionary.
* Fixed bug in :class:`stdnet.orm.Field` when using default values. Default values
  are regenerated if missing during the saving algorithm.
* Refactored redisinfo for a better redis monitor.
* **297 regression tests** with **78%** coverage.

Ver. 0.6.0 - 2011 Aug 9
============================
* **New database schema incompatible with previous versions**.
* This is a major release which brings into production a vast array
  of important new features including an improved database schema.
* :class:`stdnet.orm.StdModel` instances are mapped into separate redis hash
  tables with fields given by the model field names and values given by the
  instance field values.
* Implemented two types of sorting: 
    * *Implicit* by the :class:`stdnet.orm.Metaclass` attribute ``ordering``.
      When using this route, items are stored in the database in a sorted
      fashion, therefore no overhead is required for the sorting step.
    * *Explicit* by using the ``sort_by`` method in
      a :class:`stdnet.orm.query.QuerySet` object.
  
  Check the :ref:`sorting <sorting>` documentation for more information.
* Unique fields (fields with :attr:`stdnet.orm.Field.unique` set to ``True``)
  are now indexed via redis_ hash tables which maps the field value to the
  object id. Previously they were stored in keys. This solution
  reduces the memory footprint and the number of keys used.
* Added :ref:`transaction support <model-transactions>`.
  This way model instances are always consistent even when redis
  shuts down during an update. Transactions are also useful when updating several
  instances at once.
* Added support for hiredis_. If installed it will be used as default redis parser.
* Added :ref:`serialization utilities <serialize-models>` for saving model
  data in JSON or CSV format. Custom serialization algorithms
  can be added to the library.
* Data encoders have been moved to the :mod:`stdnet.utils.encoders` module.
  There are four available, a dummy one (no encoding), `Default` to and
  from `unicode` and `bytes`, `Bytes` to and from bytes, `PythonPickle`
  to and from object and their pickle (bytes) representation and
  `Json` to and from structures and bytes.
* Added ``as_string`` parameter to :class:`stdnet.orm.JSONField` for
  specifying the storage method.
* Moved testing functions into the :mod:`stdnet.test` module.
* Added ``hidden`` attribute to :class:`stdnet.orm.Field`.
  Used in the search algorithm.
* Reorganized and expanded documentation.
* Bug fix in :class:`stdnet.orm.PickleObjectField` field.
* **289 regression tests** with **78%** coverage.

Ver. 0.5.5 - 2011 June 6
============================
* Several new features, some important bug fixes and more tests.
* Added :func:`stdnet.orm.from_uuid` function which can be used to retrieve a model
  instance from its universally unique identifier.
* Added pickle support to models. The `__getstate__` method return a tuple containg ``id``
  and a dictionary representation of scalar fields (obtained from the ``todict`` method).
* Bug Fix in :class:`stdnet.orm.JSONField`.
* Added tests for timeseries with date as keys (rather than datetimes).
* Bug fix in Backend and test suite, Redis port was not read.
* Bug fix in :class:`stdnet.contrib.timeseries`. The models were overridding
  the :meth:`__str__` rather than :meth:`__unicode__`. 
* Added :func:`stdnet.orm.flush_models`, a utility functions for flushing model data.
* Added a new :class:`stdnet.orm.ByteField` which saves bytes rather than strings.
* Renamed ``start`` and ``end`` in TimeSeres to ``data_start`` and ``data_end``.
* **245 regression tests** with **76%** coverage.

Ver. 0.5.4 - 2011 May 18
============================
* Another bug fixing release with a couple of new functionalities and a new ``contrib`` application.
* Fixed a filtering problem when performing exclude on unique fields.
* Refactored registration utilities.
* Added :func:`stdnet.orm.test_unique` for testing uniqueness.
* Removed `tagging` from :mod:`contrib` and included in the :mod:`contrib.searchengine`.
  The search engine application has been refactored so that it can perform 
  a fast, fuzzy, full text index using Redis.
* Added ``pre_save`` and ``post_save`` signals.
* Added ``pre_delete`` and ``post_delete`` signals.
* Bug fix on ``disptach`` module which was failing when using python 3.
* Several more tests.
* **218 regression tests** with **73%** coverage.

Ver. 0.5.3 - 2011 Apr 30
=============================
* Fixed problem in setup.py.
* Added ``remove`` method to :class:`stdnet.orm.ManyToManyField` and
  fixed a bug on the same field.
* **203 regression tests** with **71%** coverage.

Ver. 0.5.2 - 2011 Mar 31
==========================
* This version brings some important bug fixes with tests and preliminary work on C extensions
  based on ``hiredis``.
* Bug fix in :meth:`stdnet.orm.IntegerField.to_python`.
* Added registration utilities in :mod:`stdnet.orm`.
* Bug fix in :class:`stdnet.orm.StdModel` class caused by the lack of a ``__ne__`` operator.
* Added ``__hash__`` operator, unique across different models, not just instances.
* Added experimental :mod:`stdnet.contrib.searchengine` application. Very much alpha.
* Added ``scorefun`` callable in structures to be used in OrderedSet.
* Added a ``spelling`` example.
* **198 regression tests (including timeseries)** with **71%** coverage.

Ver. 0.5.1 - 2011 Feb 27
==========================
* Mainly bug fixes, documentations and more tests (improved coverage).
* Modified the ``parse_info`` method in :mod:`stdnet.lib.redis`. Its now compatible with redis 2.2.
* Added documentation for :ref:`Redis timeseries <redis-timeseries>`.
* Added a command to :mod:`stdnet.contrib.monitor`, a stdnet application for djpcms_.
* Critical Bug fix in redis backend ``save_object`` attribute. This bug was causing the deletion of related objects when
  updating the value of existing objects.
* Added licences to the :mod:`stdnet.dispatch` and :mod:`stdnet.lib.redis` module.
* **177 regression tests, 189 with timeseries** with **67%** coverage.

Ver. 0.5.0 - 2011 Feb 24
===========================
* Ported to ``Python 3`` and dropped support for ``python 2.5``. Way to go.
* Removed dependency from ``redis-py`` for python 3 compatibility.
* Refactored the object relational mapper, including several bug fixes.
* Added benchmark and profile to tests. To run benchmarks or profile::

    python runtests.py -t bench
    python runtests.py -t bench tag1 tag2
    python runtests.py -t profile
    
* Included support for redis ``timeseries`` which requires redis fork at https://github.com/lsbardel/redis. 
* Added :mod:`stdnet.contrib.sessions` module for handling web sessions. Experimental and pre-alpha.
* Added :class:`stdnet.orm.JSONField` with tests.
* **167 regression tests** with **61%** coverage.

Ver. 0.4.2 - 2010 Nov 17
============================
* Added ``tags`` in tests. You can now run specific tags::

	python runtests.py hash
	
  will run tests specific to hashtables.	
* Removed ``ts`` tests since the timeseries structure is not in redis yet. You can run them by setting tag ``ts``.
* **54** tests.

Ver. 0.4.1 - 2010 Nov 14
============================
* Added ``CONTRIBUTING`` to distribution.
* Corrected spelling error in Exception ``ObjectNotFound`` exception class.
* Added initial support for ``Map`` structures. Ordered Associative Containers.
* **63 tests**


Ver. 0.4.0 - 2010 Nov 11
============================
* Development status set to ``beta``.
* **This version is incompatible with previous versions**.
* Documentation hosted at github.
* Added new ``contrib`` module ``djstdnet`` which uses `djpcms`_ content management system to display an admin
  interface for a :class:`stdnet.orm.StdModel`. Experimental for now.
* Added :class:`stdnet.CacheClass` which can be used as django_ cache backend. For example, using redis database 11 as cache is obtained by::

	CACHE_BACKEND = 'stdnet://127.0.0.1:6379/?type=redis&db=11&timeout=300'
	
* Overall refactoring of :mod:`stdnet.orm` and :mod:`stdnet.backends` modules.
* Lazy loading of models via the :mod:`stdnet.dispatch` module.
* Added :mod:`stdnet.dispatch` module from django_.
* Added :class:`stdnet.orm.AtomField` subclasses. 
* Before adding elements to a :class:`stdnet.orm.MultiField` the object needs to be saved, i.e. it needs to have a valid id.
* Made clear that :class:`stdnet.orm.StdModel` classes are mapped to :class:`stdnet.HashTable`
  structures in a :class:`stdnet.BackendDataServer`.
* Moved ``structures`` module into ``backends`` directory. Internal reorganisation of several modules.
* Added ``app_label`` attribute to :class:`stdnet.orm.DataMetaClass`.
* **47 tests**

Ver. 0.3.3 - 2010 Sep 13
========================================
* If a model is not registered and the manager method is accessed, it raises ``ModelNotRegistered``
* Changed the way tests are run. See documentation
* ``redis`` set as requirements
* **29 tests**

Ver. 0.3.2 - 2010 Aug 24
========================================
* Bug fixes
* Fixed a bug on ``orm.DateField`` when ``required`` is set to ``False``
* ``Changelog`` included in documentation
* **27 tests**

Ver. 0.3.1 - 2010 Jul 19
========================================
* Bug fixes
* **27 tests**

Ver. 0.3.0 - 2010 Jul 15
========================================
* Overall code refactoring
* Added ListField and OrderedSetField with Redis implementation
* ``StdModel`` raise ``AttributError`` when method/attribute not available. Previously it returned ``None``
* ``StdModel`` raise ``ModelNotRegistered`` when trying to save an instance of a non-registered model
* **24 tests**

Ver. 0.2.2 - 2010 Jul 7
========================================
* ``RelatedManager`` is derived by ``Manager`` and therefore implements both all and filter methods
* **10 tests**

Ver. 0.2.0  - 2010 Jun 21
========================================
* First official release in pre-alpha
* ``Redis`` backend
* Initial ``ORM`` with ``AtomField``, ``DateField`` and ``ForeignKey``
* **8 tests**


.. _redis: http://redis.io/
.. _djpcms: http://djpcms.com
.. _django: http://www.djangoproject.com/
.. _hiredis: https://github.com/pietern/hiredis-py