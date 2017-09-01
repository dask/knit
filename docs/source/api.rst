API
===

.. currentmodule:: knit.env

.. autosummary::
   CondaCreator
   CondaCreator.create_env
   CondaCreator.zip_env

.. autoclass:: CondaCreator
   :members"

.. currentmodule:: knit.yarn_api

.. autosummary::
   YARNAPI
   YARNAPI.apps
   YARNAPI.app_containers
   YARNAPI.logs
   YARNAPI.container_status
   YARNAPI.status
   YARNAPI.kill_all
   YARNAPI.kill

.. currentmodule:: knit.core

.. autosummary::
   Knit
   Knit.start
   Knit.logs
   Knit.status
   Knit.kill
   Knit.create_env

.. autoclass:: Knit
   :members:

.. py:currentmodule:: knit.dask_yarn

.. autosummary::
   DaskYARNCluster
   DaskYARNCluster.start
   DaskYARNCluster.stop
   DaskYARNCluster.close
   DaskYARNCluster.add_workers
   DaskYARNCluster.remove_worker

.. autoclass:: DaskYARNCluster
   :members: