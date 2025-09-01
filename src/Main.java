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
        String startStr = args[0];  // "2025-07-01 00:00:00"
        String endStr   = args[1];  // "2025-07-01 01:00:00"
        String tagsFile = args[2];  // "tags1.txt"
        // String outPrefix = args.length >= 4 ? args[3] : "data_origin";

        // ====== 构造输出文件名 ======
        String safeStart = startStr.replace(" ", "_").replace(":", "-");
        String safeEnd   = endStr.replace(" ", "_").replace(":", "-");

        String safeTagsFile = tagsFile;
        int dotIndex = tagsFile.lastIndexOf('.');
        if (dotIndex > 0) {
            safeTagsFile = tagsFile.substring(0, dotIndex);
        }
        
        String folderName = safeTagsFile + "_data_" + safeStart + "__" + safeEnd;
        java.io.File folder = new java.io.File(folderName);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        // ====== 打印确认信息 ======
        System.out.println("开始时间: " + startStr);
        System.out.println("截止时间: " + endStr);
        System.out.println("Tags文件: " + tagsFile);
        System.out.println("输出文件夹: " + folderName);


        List<NameTag> pairs = readNameTags(Paths.get(tagsFile));
        if (pairs.isEmpty()) {
            System.err.println("输入文件为空或未找到有效的 显示名,tag。");
            return;
        }
        System.out.println("将处理的 tag 数量: " + pairs.size());
        final int n = pairs.size();
        System.out.println("将处理的 tag 数量: " + pairs.size());

        SimpleDateFormat fmt = new SimpleDateFormat(TS_PATTERN);
        Date start = fmt.parse(startStr);
        Date end   = fmt.parse(endStr);
        // if (!start.before(end)) {
        //     System.err.println("开始时间需要早于结束时间。");
        //     return;
        // }

        int STEP_MINUTES = 5;
        List<Date[]> windows = splitByMinutes(start, end, STEP_MINUTES);


        TreeMap<Long, double[]> timeToRow = new TreeMap<>();
        
        for (int widx = 0; widx < windows.size(); widx++) {
            Date[] win = windows.get(widx);
            Date winStart = win[0];
            Date winEnd   = win[1];

            long startMs = toSecMs(winStart.getTime());
            long endMs   = toSecMs(winEnd.getTime());

            System.out.println(String.format("处理时间窗: %s ~ %s",
                    fmt.format(winStart), fmt.format(winEnd)));

            // ⭐ 关键区别：为“当前窗口”单独建一个 Map，避免全局累计
            NavigableMap<Long, double[]> perWindow = new java.util.TreeMap<>();

            // 1) 先把这个窗口内“每一秒”的行都预建好（NaN）
            ensurePerSecondRows(winStart, winEnd, n, perWindow);

            // 2) 再逐列抓取并写值
            for (int col = 0; col < n; col++) {
                NameTag nt = pairs.get(col);
                String tag = nt.tag;
                String name = nt.name;

                System.out.printf("  [%d/%d] 正在处理 tag: %s (%s)...%n", col + 1, n, name, tag);

                // List<DoubleData> list = fetchWithRetry(
                //         tag, winStart, winEnd, MAX_RETRIES, INITIAL_BACKOFF_MS, fmt);

                List<DoubleData> list = fetchUntilSuccess(
                        tag, winStart, winEnd, INITIAL_BACKOFF_MS, fmt);        

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

                    double[] row = perWindow.get(ts); // 一定存在，因为已预建
                    if (row == null) {
                        // 理论上不会发生；稳妥起见保底建一下
                        row = new double[n];
                        Arrays.fill(row, Double.NaN);
                        perWindow.put(ts, row);
                    }

                    Double val = d.getValue();
                    row[col] = (val == null ? Double.NaN : val.doubleValue());
                }
            }

            // 当前窗口立刻落盘（GBK)
            Path outPath = buildWindowCsvPath(folderName, winStart, winEnd, widx + 1);
            writeCsvGbk(outPath, fmt, pairs, perWindow);
            System.out.println("已写出 CSV: " + outPath);
            System.out.println("本窗口总行数: " + perWindow.size());
        }

        // 如果你仍想保留“汇总一份大 CSV”，可以在循环外继续使用原先的全局 timeToRow 再写一次。
        String allData = folderName + "/data_origin.csv";
        writeCsv(Paths.get(allData), fmt, pairs, timeToRow);
        System.out.println("已写出 CSV: " + allData);
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


    /** 将 [start, end) 划分为若干 [winStart, winEnd]；当 start < end 时，最末段止于 end-1s（右开到 end） */
    private static List<Date[]> splitByMinutes(Date start, Date end, int stepMinutes) {
        if (stepMinutes <= 0) {
            throw new IllegalArgumentException("stepMinutes 必须为正整数");
        }
        if (start.after(end)) {
            throw new IllegalArgumentException("start 不能晚于 end");
        }

        // 明确支持：start == end 时，返回一个零长度窗口 [start, end]
        if (start.equals(end)) {
            return Collections.singletonList(new Date[]{start, end});
        }

        // 右开到 end：把硬性终点设为 end - 1s
        Date hardEnd = new Date(end.getTime() - 1000L);

        // 若 end - 1s 仍早于 start（区间不足 1s），则没有可分的闭区间段
        if (hardEnd.before(start)) {
            return Collections.emptyList();
        }

        List<Date[]> windows = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);

        while (!cal.getTime().after(hardEnd)) { // 等价于 cal <= hardEnd
            Date winStart = cal.getTime();

            Calendar next = (Calendar) cal.clone();
            next.add(Calendar.MINUTE, stepMinutes);
            Date candidateEnd = next.getTime();

            Date winEnd = candidateEnd.before(hardEnd) ? candidateEnd : hardEnd;
            windows.add(new Date[]{winStart, winEnd});

            if (!candidateEnd.before(hardEnd)) break; // 已到或超过 hardEnd (即 end-1s)
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

    /** 只要结果为空就重试（无上限），指数退避 */
    private static List<DoubleData> fetchUntilSuccess(
            String tag, Date winStart, Date winEnd,
            long initialBackoffMs, SimpleDateFormat fmt) {

        long backoff = Math.max(1L, initialBackoffMs); // 保底 1ms
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                List<DoubleData> res = GoldenRTDBDao.getDoubleArchivedValuesByTag(tag, winStart, winEnd);
                if (res != null && !res.isEmpty()) return res;
                System.err.printf("返回空结果: tag=%s 窗口=%s~%s 尝试=%d，将在 %dms 后重试%n",
                        tag, fmt.format(winStart), fmt.format(winEnd), attempt, backoff);
            
            } catch (ExceptionInInitializerError e) {
                System.err.println("GoldenRTDBDao 静态初始化失败（不会重试）");
                if (e.getCause() != null) e.getCause().printStackTrace(); else e.printStackTrace();
                // 结束重试：要么抛出，要么返回空并让上游处理
                throw e; // 或者 return Collections.emptyList();
            
            } catch (NoClassDefFoundError e) {
                if (String.valueOf(e.getMessage())
                        .contains("Could not initialize class com.sn.parent.goldentools.component.GoldenRTDBDao")) {
                    System.err.println("GoldenRTDBDao 已因初始化失败被标记不可用（不会重试）");
                    e.printStackTrace();
                    throw e; // 或 return Collections.emptyList();
                }
                throw e;
            
            } catch (Throwable t) {
                // 仅对“可恢复”的异常重试
                t.printStackTrace();
                System.err.printf("调用异常(忽略并重试): tag=%s 窗口=%s~%s 尝试=%d，将在 %dms 后重试；原因=%s%n",
                        tag, fmt.format(winStart), fmt.format(winEnd), attempt, backoff, t);
            }
            

            try { Thread.sleep(backoff); } catch (InterruptedException ignored) {}

            // 指数退避（无上限）；如需上限可改成 Math.min(backoff << 1, 60_000L)
            // backoff = backoff << 1;
            backoff = Math.min(backoff << 1, 60_000L);
            if (backoff <= 0) backoff = Long.MAX_VALUE / 2; // 防溢出
        }
    }



    // 文件名拼接（outCsv 无后缀，这里统一补 .csv）
    private static java.nio.file.Path buildWindowCsvPath(String base, java.util.Date start, java.util.Date end, int idx) {
        java.text.SimpleDateFormat ts = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
        String name = String.format("%s/%06d_%s_%s.csv", base, idx, ts.format(start), ts.format(end));
        return java.nio.file.Paths.get(name);
    }

    // 以 GBK 编码写 CSV（不用 OutputStreamWriter / StandardOpenOption）
    private static void writeCsvGbk(java.nio.file.Path path,
                                    java.text.DateFormat fmt,
                                    java.util.List<NameTag> pairs,
                                    java.util.NavigableMap<Long, double[]> timeToRow) throws java.io.IOException {

        try (java.io.BufferedWriter w = java.nio.file.Files.newBufferedWriter(
                path, java.nio.charset.Charset.forName("GBK"))) {

            // header
            StringBuilder header = new StringBuilder("time");
            for (NameTag nt : pairs) {
                header.append(',').append(nt.name);
            }
            w.write(header.toString());
            w.newLine();

            // rows（按时间升序）
            for (java.util.Map.Entry<Long, double[]> e : timeToRow.entrySet()) {
                long tsMs = e.getKey();
                double[] row = e.getValue();

                StringBuilder line = new StringBuilder();
                line.append(fmt.format(new java.util.Date(tsMs)));
                for (double v : row) {
                    if (Double.isNaN(v)) {
                        line.append(','); // 空值
                    } else {
                        line.append(',').append(v);
                    }
                }
                w.write(line.toString());
                w.newLine();
            }
        }
    }
}
