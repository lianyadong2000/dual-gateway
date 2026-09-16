package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// 日志级别
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// 解析日志级别
func ParseLevel(level string) Level {
	switch level {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn":
		return WarnLevel
	case "error":
		return ErrorLevel
	case "fatal":
		return FatalLevel
	default:
		return InfoLevel
	}
}

// 日志格式
type Format int

const (
	TextFormat Format = iota
	JSONFormat
)

// 日志字段
type Field struct {
	Key   string
	Value interface{}
}

// 日志条目
type Entry struct {
	Time    time.Time
	Level   Level
	Message string
	Fields  []Field
	File    string
	Line    int
}

// 日志接口
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Fatal(msg string, fields ...interface{})
	WithFields(fields ...interface{}) Logger
	WithContext(ctx context.Context) Logger
	SetLevel(level Level)
	SetOutput(w io.Writer)
	Close() error
}

// 默认日志实现
type defaultLogger struct {
	mu     sync.Mutex
	level  Level
	format Format
	output io.Writer
	fields []interface{}
}

// 创建日志器
func New(level string, format string) Logger {
	logLevel := ParseLevel(level)
	logFormat := TextFormat
	if format == "json" {
		logFormat = JSONFormat
	}
	
	return &defaultLogger{
		level:  logLevel,
		format: logFormat,
		output: os.Stdout,
	}
}

// 创建文件日志器
func NewFileLogger(level, format, filePath string) (Logger, error) {
	logger := New(level, format).(*defaultLogger)
	
	// 创建目录
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}
	
	// 打开日志文件
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	
	logger.output = file
	return logger, nil
}

// Debug日志
func (l *defaultLogger) Debug(msg string, fields ...interface{}) {
	l.log(DebugLevel, msg, fields...)
}

// Info日志
func (l *defaultLogger) Info(msg string, fields ...interface{}) {
	l.log(InfoLevel, msg, fields...)
}

// Warn日志
func (l *defaultLogger) Warn(msg string, fields ...interface{}) {
	l.log(WarnLevel, msg, fields...)
}

// Error日志
func (l *defaultLogger) Error(msg string, fields ...interface{}) {
	l.log(ErrorLevel, msg, fields...)
}

// Fatal日志
func (l *defaultLogger) Fatal(msg string, fields ...interface{}) {
	l.log(FatalLevel, msg, fields...)
	os.Exit(1)
}

// 添加字段
func (l *defaultLogger) WithFields(fields ...interface{}) Logger {
	newLogger := &defaultLogger{
		level:  l.level,
		format: l.format,
		output: l.output,
		fields: append(l.fields, fields...),
	}
	return newLogger
}

// 添加上下文
func (l *defaultLogger) WithContext(ctx context.Context) Logger {
	fields := []interface{}{}
	
	// 从上下文中提取追踪ID
	if traceID := ctx.Value("trace_id"); traceID != nil {
		fields = append(fields, "trace_id", traceID)
	}
	
	// 从上下文中提取用户ID
	if userID := ctx.Value("user_id"); userID != nil {
		fields = append(fields, "user_id", userID)
	}
	
	return l.WithFields(fields...)
}

// 设置日志级别
func (l *defaultLogger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// 设置输出
func (l *defaultLogger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

// 关闭日志器
func (l *defaultLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if closer, ok := l.output.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// 记录日志
func (l *defaultLogger) log(level Level, msg string, fields ...interface{}) {
	if level < l.level {
		return
	}
	
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// 获取调用者信息
	_, file, line, _ := runtime.Caller(2)
	file = filepath.Base(file)
	
	// 合并字段
	allFields := make([]interface{}, 0, len(l.fields)+len(fields))
	allFields = append(allFields, l.fields...)
	allFields = append(allFields, fields...)
	
	entry := Entry{
		Time:    time.Now(),
		Level:   level,
		Message: msg,
		Fields:  parseFields(allFields),
		File:    file,
		Line:    line,
	}
	
	// 格式化输出
	var output string
	if l.format == JSONFormat {
		output = formatJSON(entry)
	} else {
		output = formatText(entry)
	}
	
	fmt.Fprintln(l.output, output)
}

// 解析字段
func parseFields(fields []interface{}) []Field {
	result := make([]Field, 0, len(fields)/2)
	
	for i := 0; i < len(fields)-1; i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		
		result = append(result, Field{
			Key:   key,
			Value: fields[i+1],
		})
	}
	
	return result
}

// 格式化文本日志
func formatText(entry Entry) string {
	fields := ""
	for _, field := range entry.Fields {
		fields += fmt.Sprintf(" %s=%v", field.Key, field.Value)
	}
	
	return fmt.Sprintf("%s [%s] %s:%d %s%s",
		entry.Time.Format("2006-01-02 15:04:05.000"),
		entry.Level.String(),
		entry.File,
		entry.Line,
		entry.Message,
		fields,
	)
}

// 格式化JSON日志
func formatJSON(entry Entry) string {
	fields := make(map[string]interface{})
	for _, field := range entry.Fields {
		fields[field.Key] = field.Value
	}
	
	fields["time"] = entry.Time.Format(time.RFC3339)
	fields["level"] = entry.Level.String()
	fields["file"] = fmt.Sprintf("%s:%d", entry.File, entry.Line)
	fields["message"] = entry.Message
	
	// 简单JSON序列化
	jsonStr := "{"
	first := true
	for k, v := range fields {
		if !first {
			jsonStr += ","
		}
		jsonStr += fmt.Sprintf("\"%s\":\"%v\"", k, v)
		first = false
	}
	jsonStr += "}"
	
	return jsonStr
}

// 全局日志器
var globalLogger Logger = New("info", "text")

// 设置全局日志器
func SetGlobalLogger(logger Logger) {
	globalLogger = logger
}

// 获取全局日志器
func GetGlobalLogger() Logger {
	return globalLogger
}

// 全局日志函数
func Debug(msg string, fields ...interface{}) {
	globalLogger.Debug(msg, fields...)
}

func Info(msg string, fields ...interface{}) {
	globalLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...interface{}) {
	globalLogger.Warn(msg, fields...)
}

func Error(msg string, fields ...interface{}) {
	globalLogger.Error(msg, fields...)
}

func Fatal(msg string, fields ...interface{}) {
	globalLogger.Fatal(msg, fields...)
}