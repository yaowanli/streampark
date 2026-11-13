import { useI18n } from '/@/hooks/web/useI18n';
import { BasicColumn } from '/@/components/Table';

const { t } = useI18n();

export const columns: BasicColumn[] = [
  { title: 'ID', dataIndex: 'id', width: 70 },
  { title: t('kubernetes.cluster.name'), dataIndex: 'clusterName' },
  { title: t('kubernetes.cluster.prometheusUrl'), dataIndex: 'prometheusUrl' },
  { title: t('kubernetes.cluster.description'), dataIndex: 'description' },
  { title: t('common.createTime'), dataIndex: 'createTime', width: 170 },
];
