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
        
        String folderName = "variables_out";
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

        int STEP_MINUTES = 10;
        List<Date[]> windows = splitByMinutes(start, end, STEP_MINUTES);


        TreeMap<Long, double[]> timeToRow = new TreeMap<>();

        // 确定输出根目录
        Path baseOutputRoot = Paths.get(folderName);
        Files.createDirectories(baseOutputRoot); // 确保存在
        
        for (int widx = 0; widx < windows.size(); widx++) {
            Date[] win = windows.get(widx);
            Date winStart = win[0];
            Date winEnd   = win[1];
        
            long startMs = toSecMs(winStart.getTime());
            long endMs   = toSecMs(winEnd.getTime());
        
            System.out.println(String.format("处理时间窗: %s ~ %s",
                    fmt.format(winStart), fmt.format(winEnd)));
        
            // === 关键变化点：改为“每列一个 Map”，不再全局二维累计 ===
            List<NavigableMap<Long, Double>> perColMaps = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                NavigableMap<Long, Double> perCol = new java.util.TreeMap<>();
                ensurePerSecondRowsSingleCol(winStart, winEnd, perCol); // 预建 NaN 行
                perColMaps.add(perCol);
            }


        
            // 逐列抓取并写入各自 map
            for (int col = 0; col < n; col++) {
                NameTag nt = pairs.get(col);
                String tag = nt.tag;   // 抓取用
                String name = nt.name; // 落盘目录名
        
                System.out.printf("  [%d/%d] 正在处理 tag: %s (%s)...%n", col + 1, n, name, tag);
        
                List<DoubleData> list = fetchUntilSuccess(
                        tag, winStart, winEnd, INITIAL_BACKOFF_MS, fmt);
        
                if (list == null || list.isEmpty()) {
                    System.out.printf("    -> tag %s 无数据%n", tag);
                    continue;
                }
        
                System.out.printf("    -> tag %s 返回数据点数: %d%n", tag, list.size());
        
                NavigableMap<Long, Double> perCol = perColMaps.get(col);
        
                for (DoubleData d : list) {
                    Date dt = d.getDateTime();
                    if (dt == null) continue;
        
                    long ts = toSecMs(dt.getTime());
                    if (ts < startMs || ts > endMs) continue; // 丢弃窗口外
        
                    Double val = d.getValue();
                    perCol.put(ts, (val == null ? Double.NaN : val.doubleValue()));
                }

            }
        
            // === 写盘：按列名分目录，每列一个 CSV ===
            // 用你原来的函数生成“原始路径”，从中拿到“文件名”（保持不变）
            
        
            for (int col = 0; col < n; col++) {
                String colName = pairs.get(col).name; // 目录名 = 列名
                Path original = buildWindowCsvPath(folderName, colName, winStart, winEnd);
                String fileName = extractFileName(original);
                Path colDir = baseOutputRoot.resolve(sanitizeAsFolder(colName)); // 你的输出根目录
                ensureDir(colDir);
        
                Path outPath = colDir.resolve(fileName);
                writeSingleColCsvGbk(outPath, fmt, colName, perColMaps.get(col));
        
                System.out.println("已写出列CSV: " + outPath);
                System.out.println("本列本窗口总行数: " + perColMaps.get(col).size());
            }
        }
        
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
    private static java.nio.file.Path buildWindowCsvPath(String base, String tagName, java.util.Date start, java.util.Date end) {
        java.text.SimpleDateFormat ts = new java.text.SimpleDateFormat("yyyyMMdd'T'HHmmss");
        String name = String.format("%s/%s_%s-%s.csv", base, tagName, ts.format(start), ts.format(end));
        return java.nio.file.Paths.get(name);
    }

    // 单列：预建每秒时间戳 -> NaN
    private static void ensurePerSecondRowsSingleCol(Date start, Date end,
                                                    NavigableMap<Long, Double> perCol) {
        long s = toSecMs(start.getTime());
        long e = toSecMs(end.getTime());
        for (long t = s; t <= e; t += 1000L) {
            perCol.put(t, Double.NaN);
        }
    }

    // 写单列 CSV（GBK），两列：time,<colName>
    private static void writeSingleColCsvGbk(Path outPath, SimpleDateFormat fmt, String colName,
                                            NavigableMap<Long, Double> perCol) throws IOException {
        ensureDir(outPath.getParent());
        try (BufferedWriter bw = Files.newBufferedWriter(outPath, java.nio.charset.Charset.forName("GBK"))) {
            bw.write("time," + colName);
            bw.newLine();
            for (Map.Entry<Long, Double> en : perCol.entrySet()) {
                String timeStr = fmt.format(new Date(en.getKey()));
                Double v = en.getValue();
                // 保留 NaN 字面量（或你也可以写空字符串）
                bw.write(timeStr);
                bw.write(',');
                if (v == null || v.isNaN()) {
                    bw.write("NaN");
                } else {
                    bw.write(Double.toString(v));
                }
                bw.newLine();
            }
        }
    }

    // 路径取文件名（保持你原来的命名）
    private static String extractFileName(Path p) {
        return p.getFileName().toString();
    }

    // 目录存在性保证
    private static void ensureDir(Path dir) throws IOException {
        if (dir != null && !Files.exists(dir)) {
            Files.createDirectories(dir);
        }
    }

    // 列名转安全目录名（去除不合法字符）
    private static String sanitizeAsFolder(String name) {
        // Windows 不支持 <>:"/\\|?* 等；同时去掉尾部空格和点
        String s = name.replaceAll("[<>:\"/\\\\|?*]+", "_").trim();
        while (s.endsWith(".") || s.endsWith(" ")) {
            s = s.substring(0, s.length() - 1);
        }
        if (s.isEmpty()) s = "_";
        return s;
    }

}
