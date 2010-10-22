Ver. 0.4 - Development
============================
* Development status set to ``beta``.
* **This version is incompatible with previous versions**.
* Added :class:`stdnet.orm.AtomField` subclasses. 
* Before adding elements to a :class:`stdnet.orm.MultiField` the object needs to be saved, i.e. it needs to have a valid id.
* Made clear that :class:`stdnet.orm.StdModel` classes are mapped to :class:`stdnet.HashTable`
  structures in a :class:`stdnet.BackendDataServer`. 
* Moved ``structures`` module into ``backends`` directory. Internal reorganisation of several modules.
* Added ``app_label`` attribute to :class:`stdnet.orm.DataMetaClass`.
* Added a new module ``stdnet.contrib.monitor`` for monitoring objects on the web. The module requires djpcms_.
* **31 tests**

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


.. _djpcms: http://djpcms.com