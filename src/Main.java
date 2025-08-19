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
//     static final String START_STR = "2025-07-01 00:00:00";
//     static final String END_STR   = "2025-07-01 01:00:00";
//     static final String TAGS_FILE = "tags1.txt";
//     static final String OUT_CSV   = "data_origin1.csv";
    static final String TS_PATTERN = "yyyy-MM-dd HH:mm:ss";

    // Retry settings
    static final int MAX_RETRIES = 3;          // total attempts = MAX_RETRIES
    static final long INITIAL_BACKOFF_MS = 300; // 300ms, then 600ms, then 1200ms...

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("用法: java Main <开始时间> <截止时间> <tags文件名> [输出文件前缀]");
            System.out.println("示例: java Main \"2025-07-01 00:00:00\" \"2025-07-01 01:00:00\" tags1.txt data_origin");
            return;
        }

        // ====== 从命令行获取参数 ======
        String START_STR = args[0];  // "2025-07-01 00:00:00"
        String END_STR   = args[1];  // "2025-07-01 01:00:00"
        String TAGS_FILE = args[2];  // "tags1.txt"
        String outPrefix = args.length >= 4 ? args[3] : "data_origin";

        // ====== 构造输出文件名 ======
        String safeStart = startStr.replace(" ", "_").replace(":", "-");
        String safeEnd   = endStr.replace(" ", "_").replace(":", "-");
        String outCsv = String.format("%s_%s__%s__%s.csv", outPrefix, safeStart, safeEnd, tagsFile);

        // ====== 打印确认信息 ======
        System.out.println("开始时间: " + startStr);
        System.out.println("截止时间: " + endStr);
        System.out.println("Tags文件: " + tagsFile);
        System.out.println("输出文件: " + outCsv);


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

        int STEP_MINUTES = 5;
        List<Date[]> windows = splitByMinutes(start, end, STEP_MINUTES);


        TreeMap<Long, double[]> timeToRow = new TreeMap<>();

        for (Date[] win : windows) {
            Date winStart = win[0];
            Date winEnd   = win[1];

            long startMs = toSecMs(winStart.getTime());
            long endMs   = toSecMs(winEnd.getTime());

            System.out.println(String.format("处理时间窗: %s ~ %s",
                    fmt.format(winStart), fmt.format(winEnd)));

            // 1) 先把这个窗口内“每一秒”的行都预建好（NaN）
            ensurePerSecondRows(winStart, winEnd, n, timeToRow);

            // 2) 再逐列抓取并写值
            for (int col = 0; col < n; col++) {
                NameTag nt = pairs.get(col);
                String tag = nt.tag;
                String name = nt.name;

                System.out.printf("  [%d/%d] 正在处理 tag: %s (%s)...%n", col + 1, n, name, tag);

                List<DoubleData> list = fetchWithRetry(
                        tag, winStart, winEnd, MAX_RETRIES, INITIAL_BACKOFF_MS, fmt);

                if (list == null || list.isEmpty()) {
                    System.out.printf("    -> tag %s 无数据%n", tag);
                    continue;
                }

                System.out.printf("    -> tag %s 返回数据点数: %d%n", tag, list.size());

                for (DoubleData d : list) {
                    Date dt = d.getDateTime();
                    if (dt == null) continue;

                    // 归一到“秒”的时间戳，确保与预建行对齐
                    long ts = toSecMs(dt.getTime());

                    // 可选：丢弃落在窗口外的数据点（有些数据源会回多/回少）
                    if (ts < startMs || ts > endMs) continue;

                    double[] row = timeToRow.get(ts); // 一定存在，因为已预建
                    if (row == null) {
                        // 理论上不会发生；稳妥起见保底建一下
                        row = new double[n];
                        Arrays.fill(row, Double.NaN);
                        timeToRow.put(ts, row);
                    }

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

    // 工具方法：把毫秒时间戳归一到整秒（毫秒清零）
    private static long toSecMs(long t) {
        return (t / 1000) * 1000;
    }

    // 工具方法：为 [start,end] 窗口内的每一秒预建一行（NaN）
    private static void ensurePerSecondRows(
            Date winStart, Date winEnd, int n, Map<Long, double[]> timeToRow) {
        long startMs = toSecMs(winStart.getTime());
        long endMs   = toSecMs(winEnd.getTime());
        for (long ts = startMs; ts <= endMs; ts += 1000) { // 包含末尾；如需半开区间改成 ts < endMs
            timeToRow.computeIfAbsent(ts, k -> {
                double[] arr = new double[n];
                Arrays.fill(arr, Double.NaN);
                return arr;
            });
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


    /** 将 [start, end] 按 stepMinutes 分钟切分为若干 [winStart, winEnd]（右开到 end） */
    private static List<Date[]> splitByMinutes(Date start, Date end, int stepMinutes) {
        if (stepMinutes <= 0) {
            throw new IllegalArgumentException("stepMinutes 必须为正整数");
        }
        List<Date[]> windows = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);

        // 和原逻辑一致：最后一段到 end 为止
        while (cal.getTime().before(end)) {
            Date winStart = cal.getTime();

            Calendar next = (Calendar) cal.clone();
            next.add(Calendar.MINUTE, stepMinutes);
            Date candidateEnd = next.getTime();

            Date winEnd = candidateEnd.before(end) ? candidateEnd : end;
            windows.add(new Date[]{winStart, winEnd});

            if (!candidateEnd.before(end)) break; // 已到或超过 end
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
