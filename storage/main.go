package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

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
	serviceConfig := ServiceConfig{
		Namespace: os.Getenv("K8S_NAMESPACE"),
		Name:      os.Getenv("K8S_NAME"),
		PodName:   os.Getenv("K8S_POD"),
		Id:        0,
	}
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer throwError(logger.Sync())
	zap.ReplaceGlobals(logger)

	idS := os.Getenv("RAFT_ID")
	if idS != "" {
		id, err := strconv.Atoi(idS)
		if err != nil {
			log.Fatalf("value of RAFT_ID is not a valid integer")
		}
		serviceConfig.Id = uint32(id + 1)
	}

	addr := os.Getenv("STORAGE_ADDR")
	if addr == "" {
		addr = "[::]:80"
	}
	zap.S().Debugw("config", "addr", addr)
	raftAddrS := os.Getenv("STORAGE_RAFT_PORT")
	if raftAddrS == "" {
		raftAddrS = "5000"
	}
	raftPort, err := strconv.Atoi(raftAddrS)
	if err != nil {
		zap.S().Fatal(err)
	}
	go func() {
		throwError(LaunchRaftWithContext(uint16(raftPort), serviceConfig, context.Background()))
	}()

	counter := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storage", Name: "inserted_entries"})
	if err := prometheus.Register(counter); err != nil {
		zap.S().Fatal(err)
	}
	insertCounter = &counter

	counter = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storage", Name: "updated_entries"})
	if err := prometheus.Register(counter); err != nil {
		zap.S().Fatal(err)
	}
	updateCounter = &counter

	counter = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storage", Name: "deleted_entries"})
	if err := prometheus.Register(counter); err != nil {
		zap.S().Fatal(err)
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
			if v.URI == "/health" {
				return nil
			}
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
		if raftServer == nil {
			return c.String(http.StatusServiceUnavailable, "503 Service Unavailable")
		}
		return c.String(http.StatusOK, "200 OK")
	})
	e.POST("/", func(c echo.Context) error {
		content, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		} else if err := c.Request().Body.Close(); err != nil {
			return fmt.Errorf("close body: %w", err)
		}

		if raftServer == nil {
			return c.String(http.StatusServiceUnavailable, "503 Service Unavailable")
		}

		newId, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("generate UUID: %w", err)
		}
		if _, ok := raftState.Get(newId); ok {
			return errors.New("generate unique UUID")
		}

		command := RaftCommand{
			Id:     newId,
			Data:   content,
			Delete: false,
		}
		if err := raftServer.Update([]RaftCommand{command}); err != nil {
			return fmt.Errorf("raft update: %w", err)
		}
		return c.String(http.StatusCreated, newId.String())
	})
	e.GET("/:id", func(c echo.Context) error {
		l := logger.Sugar()
		idParam := c.Param("id")
		id, err := uuid.Parse(idParam)
		if err != nil {
			l.Debugw("invalid id param", "error", err)
			return c.String(http.StatusBadRequest, "400 Bad Request")
		}
		if raftServer == nil {
			return c.String(http.StatusServiceUnavailable, "503 Service Unavailable")
		}
		if content, ok := raftState.Get(id); ok {
			return c.Blob(http.StatusOK, "text/plain", content)
		}
		return c.String(http.StatusNotFound, "404 Not Found")
	})
	e.PUT("/:id", func(c echo.Context) error {
		l := logger.Sugar()

		idParam := c.Param("id")
		id, err := uuid.Parse(idParam)
		if err != nil {
			l.Debugw("invalid id param", "error", err)
			return c.String(http.StatusBadRequest, "400 Bad Request")
		}

		if raftServer == nil {
			return c.String(http.StatusServiceUnavailable, "503 Service Unavailable")
		}
		if _, ok := raftState.Get(id); !ok {
			return c.String(http.StatusNotFound, "404 Not Found")
		}

		content, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return err
		}
		defer throwError(c.Request().Body.Close())

		command := RaftCommand{
			Id:     id,
			Data:   content,
			Delete: false,
		}
		if err := raftServer.Update([]RaftCommand{command}); err != nil {
			return fmt.Errorf("raft update: %w", err)
		}
		return c.String(http.StatusOK, "200 OK")
	})
	e.DELETE("/:id", func(c echo.Context) error {
		l := logger.Sugar()
		idParam := c.Param("id")
		id, err := uuid.Parse(idParam)
		if err != nil {
			l.Debugw("invalid id param", "error", err)
			return c.String(http.StatusBadRequest, "400 Bad Request")
		}

		if raftServer == nil {
			return c.String(http.StatusServiceUnavailable, "503 Service Unavailable")
		}
		if _, ok := raftState.Get(id); !ok {
			return c.String(http.StatusNotFound, "404 Not Found")
		}

		command := RaftCommand{
			Id:     id,
			Data:   nil,
			Delete: true,
		}
		if err := raftServer.Update([]RaftCommand{command}); err != nil {
			return fmt.Errorf("raft update: %w", err)
		}
		return c.String(http.StatusOK, "200 OK")
	})
	zap.S().Fatal(e.Start(addr))
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
