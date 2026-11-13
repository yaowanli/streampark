import { formatToDateTime } from '/@/utils/dateUtil';

export const formatCpu = (millis?: number) => {
  if (millis == null || millis < 0) return '—';
  return millis >= 1000 ? `${(millis / 1000).toFixed(1)} cores` : `${millis}m`;
};

export const formatMemory = (bytes?: number) => {
  if (bytes == null || bytes < 0) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(1)} ${units[i]}`;
};

export const formatTimestamp = (ts?: string) => (ts ? formatToDateTime(ts) : '—');
