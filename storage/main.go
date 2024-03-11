package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/go-errors/errors"
	"github.com/google/uuid"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	logger *zap.Logger

	insertCounter *prometheus.Counter
	updateCounter *prometheus.Counter
	deleteCounter *prometheus.Counter
)

func main() {
	sysLogger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer throwError(sysLogger.Sync())
	logger = sysLogger
	l := *sysLogger.Sugar()

	addr := os.Getenv("STORAGE_ADDR")
	if addr == "" {
		addr = "[::]:3000"
	}
	l.Debugw("config", "addr", addr)

	counter := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storage", Name: "inserted_entries"})
	if err := prometheus.Register(counter); err != nil {
		l.Fatal(err)
	}
	insertCounter = &counter

	counter = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storage", Name: "updated_entries"})
	if err := prometheus.Register(counter); err != nil {
		l.Fatal(err)
	}
	updateCounter = &counter

	counter = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storage", Name: "deleted_entries"})
	if err := prometheus.Register(counter); err != nil {
		l.Fatal(err)
	}
	deleteCounter = &counter

	e := echo.New()
	e.Use(middleware.Gzip())
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogMethod:        true,
		LogStatus:        true,
		LogURI:           true,
		LogLatency:       true,
		LogContentLength: true,
		LogError:         true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			methodField := zap.String("method", v.Method)
			statusField := zap.Int("status", v.Status)
			uriField := zap.String("uri", v.URI)
			latencyField := zap.Duration("latency", v.Latency)
			contentLengthField := zap.String("contentLength", v.ContentLength)
			if v.Error != nil {
				errorField := zap.Error(v.Error)
				logger.Error("request", methodField, statusField, uriField, latencyField, contentLengthField, errorField)
			} else {
				logger.Info("request", methodField, statusField, uriField, latencyField, contentLengthField)
			}
			return nil
		},
	}))
	e.Use(echoprometheus.NewMiddlewareWithConfig(echoprometheus.MiddlewareConfig{
		Namespace: "storage",
	}))

	e.GET("/metrics", echoprometheus.NewHandler())
	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})
	e.POST("/", func(c echo.Context) error {
		l := logger.Sugar()

		content, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return err
		}
		defer throwError(c.Request().Body.Close())
		l.Infow("", "contentLength", len(content))
		// TODO
		return nil
	})
	e.GET("/:id", func(c echo.Context) error {
		l := logger.Sugar()
		idParam := c.Param("id")
		id, err := uuid.Parse(idParam)
		if err != nil {
			l.Debugw("invalid id param", "error", err)
			return c.String(http.StatusBadRequest, "400 Bad Request")
		}
		l.Info(id)
		// TODO
		return nil
	})
	e.PUT("/:id", func(c echo.Context) error {
		l := logger.Sugar()
		idParam := c.Param("id")
		id, err := uuid.Parse(idParam)
		if err != nil {
			l.Debugw("invalid id param", "error", err)
			return c.String(http.StatusBadRequest, "400 Bad Request")
		}
		l.Info(id)
		// TODO
		return nil
	})
	e.DELETE("/:id", func(c echo.Context) error {
		l := logger.Sugar()
		idParam := c.Param("id")
		id, err := uuid.Parse(idParam)
		if err != nil {
			l.Debugw("invalid id param", "error", err)
			return c.String(http.StatusBadRequest, "400 Bad Request")
		}
		l.Info(id)
		// TODO
		return nil
	})

	l.Fatal(e.Start(addr))
}

func throwError(err error) {
	if err == nil {
		return
	}
	stack := errors.New(err).StackFrames()
	fmt.Fprintf(os.Stderr, "Error Thrown: %s\nBacktrace:\n", err.Error())
	for i, frame := range stack {
		if i == 0 {
			continue
		}
		fmt.Fprintf(os.Stderr, "%4d: %s:%d\n", i, frame.File, frame.LineNumber)
	}
}
