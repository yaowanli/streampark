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
  <BasicModal :width="700" v-bind="$attrs" centered destroyOnClose @register="registerModal" showFooter @ok="handleSubmit">
    <template #title>
      <Icon icon="ant-design:cluster-outlined" />
      {{ getTitle }}
    </template>
    <div class="mt-3">
      <BasicForm @register="registerForm" :schemas="schemas" />
    </div>
    <template #insertFooter>
      <a-button type="primary" ghost @click="handleTest">
        {{ t('kubernetes.cluster.test') }}
      </a-button>
    </template>
  </BasicModal>
</template>
<script lang="ts">
  import { defineComponent, computed, ref } from 'vue';
  import { BasicForm, FormSchema, useForm } from '/@/components/Form';
  import { BasicModal, useModalInner } from '/@/components/Modal';
  import { fetchK8sClusterCreate, fetchK8sClusterTest, fetchK8sClusterUpdate } from '/@/api/kubernetes/cluster';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useMessage } from '/@/hooks/web/useMessage';
  import Icon from '/@/components/Icon';

  export default defineComponent({
    name: 'K8sClusterModal',
    components: { Icon, BasicModal, BasicForm },
    emits: ['success', 'register'],
    setup(_, { emit }) {
      const { t } = useI18n();
      const { createMessage } = useMessage();
      const isUpdate = ref(false);
      const schemas: FormSchema[] = [
        {
          field: 'id',
          label: 'id',
          component: 'Input',
          show: false,
        },
        {
          field: 'clusterName',
          label: t('kubernetes.cluster.name'),
          component: 'Input',
          required: true,
          componentProps: { placeholder: t('kubernetes.cluster.namePlaceholder') },
        },
        {
          field: 'kubeconfig',
          label: t('kubernetes.cluster.kubeconfig'),
          component: 'InputTextArea',
          required: true,
          componentProps: { rows: 10, placeholder: t('kubernetes.cluster.kubeconfigTip') },
        },
        {
          field: 'prometheusUrl',
          label: t('kubernetes.cluster.prometheusUrl'),
          component: 'Input',
          componentProps: { placeholder: t('kubernetes.cluster.prometheusUrlTip') },
        },
        {
          field: 'description',
          label: t('kubernetes.cluster.description'),
          component: 'InputTextArea',
          componentProps: { rows: 2 },
        },
      ];

      const [registerForm, { resetFields, setFieldsValue, validate, getFieldsValue }] = useForm({
        name: 'K8sClusterEditForm',
        colon: true,
        showActionButtonGroup: false,
        layout: 'vertical',
        baseColProps: { span: 22, offset: 1 },
      });

      const [registerModal, { setModalProps, closeModal }] = useModalInner(async (data: Recordable) => {
        resetFields();
        setModalProps({ confirmLoading: false });
        isUpdate.value = !!data?.isUpdate;
        if (isUpdate.value) {
          await setFieldsValue(data.record);
        }
      });

      const getTitle = computed(() =>
        !isUpdate.value ? t('kubernetes.cluster.create') : t('kubernetes.cluster.modify'),
      );

      async function handleTest() {
        const values = await getFieldsValue();
        if (!values.kubeconfig) {
          createMessage.warning(t('kubernetes.cluster.kubeconfigTip'));
          return;
        }
        try {
          const res = await fetchK8sClusterTest({ kubeconfig: values.kubeconfig });
          if (res.connected) {
            createMessage.success(`${t('kubernetes.cluster.testOk')} (${res.version})`);
          } else {
            createMessage.error(`${t('kubernetes.cluster.testFail')}: ${res.message}`);
          }
        } catch (e) {
          createMessage.error(t('kubernetes.cluster.testFail'));
        }
      }

      async function handleSubmit() {
        try {
          const values = await validate();
          setModalProps({ confirmLoading: true });
          if (isUpdate.value) {
            await fetchK8sClusterUpdate(values);
          } else {
            delete values.id;
            await fetchK8sClusterCreate(values);
          }
          closeModal();
          emit('success', isUpdate.value);
        } finally {
          setModalProps({ confirmLoading: false });
        }
      }

      return { registerModal, registerForm, schemas, getTitle, handleSubmit, handleTest, t };
    },
  });
</script>
