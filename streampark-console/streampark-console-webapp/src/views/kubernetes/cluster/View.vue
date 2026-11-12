<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<template>
  <PageWrapper content-full-height fixed-height>
    <BasicTable @register="registerTable" class="flex flex-col">
      <template #toolbar>
        <a-button type="primary" @click="handleCreate" v-auth="'k8sCluster:create'">
          <Icon icon="ant-design:plus-outlined" />
          {{ t('common.add') }}
        </a-button>
      </template>
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'action'">
          <TableAction
            :actions="[
              {
                icon: 'clarity:note-edit-line',
                auth: 'k8sCluster:update',
                tooltip: t('common.edit'),
                onClick: handleEdit.bind(null, record),
              },
              {
                icon: 'ant-design:delete-outlined',
                color: 'error',
                tooltip: t('common.delText'),
                auth: 'k8sCluster:delete',
                popConfirm: {
                  title: t('kubernetes.cluster.deleteConfirm'),
                  confirm: handleDelete.bind(null, record),
                },
              },
            ]"
          />
        </template>
      </template>
    </BasicTable>
    <ClusterModal @register="registerModal" @success="handleSuccess" />
  </PageWrapper>
</template>
<script lang="ts">
  import { defineComponent } from 'vue';
  import { BasicTable, useTable, TableAction } from '/@/components/Table';
  import ClusterModal from './ClusterModal.vue';
  import { useModal } from '/@/components/Modal';
  import Icon from '/@/components/Icon';
  import { columns } from './index.data';
  import { fetchK8sClusterDelete, fetchK8sClusterList } from '/@/api/kubernetes/cluster';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { PageWrapper } from '/@/components/Page';

  export default defineComponent({
    name: 'K8sCluster',
    components: { BasicTable, ClusterModal, TableAction, Icon, PageWrapper },
    setup() {
      const [registerModal, { openModal }] = useModal();
      const { createMessage } = useMessage();
      const { t } = useI18n();
      const [registerTable, { reload }] = useTable({
        api: fetchK8sClusterList,
        columns,
        rowKey: 'id',
        pagination: false,
        useSearchForm: false,
        showTableSetting: false,
        clickToRowSelect: false,
        actionColumn: {
          width: 120,
          title: t('common.action'),
          dataIndex: 'action',
        },
      });

      function handleCreate() {
        openModal(true, { isUpdate: false });
      }

      function handleEdit(record: Recordable) {
        openModal(true, { isUpdate: true, record });
      }

      async function handleDelete(record: Recordable) {
        const res = await fetchK8sClusterDelete(record.id);
        if (res?.status === 0) {
          createMessage.success(t('common.operateTip'));
          reload();
        }
      }

      function handleSuccess() {
        reload();
      }

      return {
        registerTable,
        registerModal,
        handleCreate,
        handleEdit,
        handleDelete,
        handleSuccess,
        t,
      };
    },
  });
</script>
