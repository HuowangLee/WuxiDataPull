import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import com.sn.parent.goldentools.component.GoldenRTDBDao;
import com.rtdb.model.DoubleData;

public class Main {

    // ======= Configurable parameters =======
    static final String START_STR = "2025-07-01 00:00:00";
    static final String END_STR   = "2025-08-01 09:00:00";
    static final String TAGS_FILE = "tags.txt";
    static final String OUT_CSV   = "data_origin.csv";
    static final String TS_PATTERN = "yyyy-MM-dd HH:mm:ss";

    // Retry settings
    static final int MAX_RETRIES = 3;          // total attempts = MAX_RETRIES
    static final long INITIAL_BACKOFF_MS = 300; // 300ms, then 600ms, then 1200ms...

    public static void main(String[] args) throws Exception {
        List<NameTag> pairs = readNameTags(Paths.get(TAGS_FILE));
        if (pairs.isEmpty()) {
            System.err.println("输入文件为空或未找到有效的 显示名,tag。");
            return;
        }
        System.out.println("将处理的 tag 数量: " + pairs.size());
        final int n = pairs.size();
        System.out.println("将处理的 tag 数量: " + pairs.size());

        SimpleDateFormat fmt = new SimpleDateFormat(TS_PATTERN);
        Date start = fmt.parse(START_STR);
        Date end   = fmt.parse(END_STR);
        if (!start.before(end)) {
            System.err.println("开始时间需要早于结束时间。");
            return;
        }

        List<Date[]> hourlyWindows = splitByHour(start, end);

        TreeMap<Long, double[]> timeToRow = new TreeMap<>();

        for (Date[] win : hourlyWindows) {
            Date winStart = win[0];
            Date winEnd   = win[1];
            System.out.println(String.format("处理时间窗: %s ~ %s",
                    fmt.format(winStart), fmt.format(winEnd)));

            for (int col = 0; col < n; col++) {
                NameTag nt = pairs.get(col);
                String tag = nt.tag;
                String name = nt.name;
            
                System.out.printf("  [%d/%d] 正在处理 tag: %s (%s)...%n", col + 1, n, name, tag);
            
                List<DoubleData> list = fetchWithRetry(tag, winStart, winEnd,
                        MAX_RETRIES, INITIAL_BACKOFF_MS, fmt);
            
                if (list == null || list.isEmpty()) {
                    System.out.printf("    -> tag %s 无数据%n", tag);
                    continue;
                }
            
                System.out.printf("    -> tag %s 返回数据点数: %d%n", tag, list.size());
            
                for (DoubleData d : list) {
                    Date dt = d.getDateTime();
                    if (dt == null) continue;
                    long ts = dt.getTime();
            
                    double[] row = timeToRow.computeIfAbsent(ts, k -> {
                        double[] arr = new double[n];
                        Arrays.fill(arr, Double.NaN);
                        return arr;
                    });
            
                    Double val = d.getValue();
                    row[col] = (val == null ? Double.NaN : val.doubleValue());
                }
            }
                    
        }

        writeCsv(Paths.get(OUT_CSV), fmt, pairs, timeToRow);
        System.out.println("已写出 CSV: " + OUT_CSV);
        System.out.println("总行数(含不同时间点): " + timeToRow.size());
    }

    static class NameTag {
        final String name; // CSV 表头显示名
        final String tag;  // 实际查询用的 tag
        NameTag(String name, String tag) {
            this.name = name;
            this.tag  = tag;
        }
    }
    
    private static List<NameTag> readNameTags(Path path) throws IOException {
        if (!Files.exists(path)) return Collections.emptyList();
    
        // 用 LinkedHashMap 去重并保持输入顺序：key=tag, value=name
        Map<String, String> tagToName = new LinkedHashMap<>();
    
        for (String raw : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
    
            int comma = line.indexOf(',');
            if (comma <= 0 || comma == line.length() - 1) {
                // 没有逗号，或逗号在开头/结尾 -> 跳过非法行
                continue;
            }
            String name = line.substring(0, comma).trim();
            String tag  = line.substring(comma + 1).trim();
            if (name.isEmpty() || tag.isEmpty()) continue;
    
            // 去重：同一个 tag 只保留第一次出现
            tagToName.putIfAbsent(tag, name);
        }
    
        List<NameTag> list = new ArrayList<>();
        for (Map.Entry<String, String> e : tagToName.entrySet()) {
            list.add(new NameTag(e.getValue(), e.getKey()));
        }
        return list;
    }
    

    private static List<Date[]> splitByHour(Date start, Date end) {
        List<Date[]> windows = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);

        while (true) {
            Date winStart = cal.getTime();

            Calendar next = (Calendar) cal.clone();
            next.add(Calendar.HOUR_OF_DAY, 1);
            Date candidateEnd = next.getTime();

            Date winEnd = candidateEnd.before(end) ? candidateEnd : end;
            windows.add(new Date[]{winStart, winEnd});

            if (!candidateEnd.before(end)) break;
            cal = next;
        }
        return windows;
    }

    private static void writeCsv(Path outPath, SimpleDateFormat fmt, List<NameTag> pairs, TreeMap<Long, double[]> timeToRow)
            throws IOException {

        try (BufferedWriter w = Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
            // header
            String header = "time," + pairs.stream().map(p -> p.name).collect(Collectors.joining(","));

            w.write(header);
            w.newLine();

            final int n = pairs.size();
            for (Map.Entry<Long, double[]> e : timeToRow.entrySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append(fmt.format(new Date(e.getKey())));

                double[] row = e.getValue();
                for (int i = 0; i < n; i++) {
                    sb.append(',');
                    double v = row[i];
                    if (!Double.isNaN(v)) {
                        sb.append(v);
                    }
                }

                w.write(sb.toString());
                w.newLine();
            }
        }
    }

    /**
     * Fetch with retry for transient socket/protocol errors from GoldenRTDB.
     */
    private static List<DoubleData> fetchWithRetry(String tag, Date winStart, Date winEnd,
                                               int maxRetries, long initialBackoffMs,
                                               SimpleDateFormat fmt) {
        long backoff = initialBackoffMs;
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return GoldenRTDBDao.getDoubleArchivedValuesByTag(tag, winStart, winEnd);
            } catch (IndexOutOfBoundsException e) {
                System.err.printf("读取失败(可能断连/协议问题): tag=%s 窗口=%s~%s 尝试=%d/%d, 错误=%s%n",
                        tag, fmt.format(winStart), fmt.format(winEnd), attempt, maxRetries, e.toString());
            } catch (RuntimeException e) { // 兜底：第三方库常把 I/O 异常包装成 RuntimeException 抛出
                System.err.printf("运行时异常: tag=%s 窗口=%s~%s 尝试=%d/%d, 错误=%s%n",
                        tag, fmt.format(winStart), fmt.format(winEnd), attempt, maxRetries, e.toString());
            }

            if (attempt >= maxRetries) {
                System.err.printf("放弃本窗口: tag=%s 窗口=%s~%s（已重试 %d 次）%n",
                        tag, fmt.format(winStart), fmt.format(winEnd), maxRetries);
                return Collections.emptyList();
            }

            try { Thread.sleep(backoff); } catch (InterruptedException ignored) {}
            backoff *= 2; // 指数退避
        }
    }

}
