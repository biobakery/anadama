AnADAMA
#######

*Another Automated Data Analysis Management Application*

.. contents::

________________________________________________________________________________

Using Version 2
===============

To start working with the new features of AnADAMA, use the ``v2`` branch.

::

  $ git clone https://github.com/biobakery/anadama.git
  $ cd anadama
  $ git checkout v2
  $ python setup.py install



Overview
========

AnADAMA is essentially doit_ with a few extensions:

- Extra command to serialize the DoIt action plan into a JSON document
- All DoIt tasks are serialized into runnable python scripts.
- Executes tasks on compute clusters LSF and SLURM without having to
  directly interact with the queueing system.
- Defines pipelines, collections of doit tasks, and provides interfaces to pipelines.

.. _doit: http://pydoit.org/

Installation
============

One liner::

  $ pip install -e 'git+https://github.com/biobakery/anadama.git@master#egg=anadama-0.0.1'


Usage
=====

Looking to use AnADAMA for microbiome sequence analysis?
Check out the ``anadama_workflows`` repository_ over on github.
