import { useI18n } from '/@/hooks/web/useI18n';
import { BasicColumn } from '/@/components/Table';

export const columns = (): BasicColumn[] => {
  const { t } = useI18n();
  return [
    { title: 'ID', dataIndex: 'id', width: 70 },
    { title: t('kubernetes.cluster.name'), dataIndex: 'clusterName' },
    { title: t('kubernetes.cluster.prometheusUrl'), dataIndex: 'prometheusUrl' },
    { title: t('kubernetes.cluster.description'), dataIndex: 'description' },
    { title: t('kubernetes.monitor.createTime'), dataIndex: 'createTime', width: 170 },
  ];
};
