package main

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/go-errors/errors"
	"github.com/robert-oleynik/k8s-playground/tester/config"
	"k8s.io/client-go/kubernetes"
)

type Ui struct {
	Config      *config.Config
	Current     int
	TestCases   []TestCase
	TestReports []*TestReport
	Tester      *Tester
	spinner     spinner.Model
	err         *errors.Error
}

func Init(conf *config.Config) *Ui {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().
		PaddingLeft(1).
		PaddingRight(0).
		MarginRight(0)
	return &Ui{
		Current:     -4,
		TestCases:   []TestCase{},
		TestReports: []*TestReport{},
		Tester:      nil,
		Config:      conf,
		spinner:     s,
	}
}

func (ui *Ui) Init() tea.Cmd {
	return tea.Batch(ui.spinner.Tick, k8sConnect(ui.Config))
}

func (ui *Ui) Register(test TestCase) {
	ui.TestCases = append(ui.TestCases, test)
	ui.TestReports = append(ui.TestReports, nil)
}

func (ui *Ui) RunTest() tea.Cmd {
	testCase := ui.TestCases[ui.Current]
	return ui.Tester.Run(testCase)
}

func (ui *Ui) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case *kubernetes.Clientset:
		ui.Current++
		if ui.Current == -3 {
			return ui, k8sRestartCluster(msg, ui.Config)
		} else if ui.Current == -2 {
			return ui, k8sConnectPeers(msg, ui.Config)
		}
	case []NodeProxy:
		ui.Tester = NewTester(msg...)
		ui.Current = -1
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		return ui, clusterBecomesReady(ctx, ui.Tester.ClusterNodes)
	case Ready:
		if !msg {
			ui.err = errors.New("cluster not ready")
			return ui, tea.Quit
		}
		ui.Current = 0
		return ui, ui.RunTest()
	case *TestReport:
		ui.TestReports[ui.Current] = msg
		ui.Current++
		if ui.Current == len(ui.TestCases) {
			return ui, tea.Quit
		}
		return ui, ui.RunTest()
	case error:
		ui.err = errors.New(msg)
		return ui, tea.Quit
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			return ui, tea.Quit
		}
	default:
		var cmd tea.Cmd
		ui.spinner, cmd = ui.spinner.Update(msg)
		return ui, cmd
	}
	return ui, nil
}

func (ui *Ui) View() string {
	baseStyle := lipgloss.NewStyle().
		Bold(true).
		PaddingLeft(1).
		PaddingRight(1).
		Width(1)
	done := baseStyle.Foreground(lipgloss.Color("2")).Render("✔")
	failed := baseStyle.Foreground(lipgloss.Color("1")).Render("✕")
	scheduled := lipgloss.NewStyle().
		Foreground(lipgloss.Color("8"))
	output := "\n"
	if ui.Current == -4 {
		if ui.err == nil {
			output += ui.spinner.View()
		} else {
			output += failed
		}
	} else {
		output += done
	}
	output += "Connecting to Kubernetes\n"

	title := "Restart Cluster Nodes"
	if ui.Current < -3 {
		output += scheduled.Render(" . " + title)
	} else if ui.Current == -3 {
		if ui.err == nil {
			output += ui.spinner.View()
		} else {
			output += failed
		}
		output += title
	} else {
		output += done + title
	}
	output += "\n"

	title = "Setup Cluster Proxies"
	if ui.Current < -2 {
		output += scheduled.Render(" . " + title)
	} else if ui.Current == -2 {
		if ui.err == nil {
			output += ui.spinner.View()
		} else {
			output += failed
		}
		output += title
	} else {
		output += done + title
	}
	output += "\n"

	title = "Wait for Cluster to become ready"
	if ui.Current < -1 {
		output += scheduled.Render(" . " + title)
	} else if ui.Current == -1 {
		if ui.err == nil {
			output += ui.spinner.View()
		} else {
			output += failed
		}
		output += title
	} else {
		output += done + title
	}
	output += "\n"

	if ui.Current >= 0 {
		output += "\nRun Tests:\n"
		for i, report := range ui.TestReports {
			if i == ui.Current {
				output += ui.spinner.View()
			} else if report == nil {
				output += " . "
			} else if report.Passed {
				output += done
			} else if report.Error != nil {
				output += failed + ui.TestCases[i].Name() + "\n"
				output += lipgloss.NewStyle().
					Margin(0).
					MarginLeft(4).
					MarginRight(4).
					Render(renderError(report.Error)) + "\n"
				continue
			} else {
				output += failed
			}
			output += ui.TestCases[i].Name() + "\n"
		}
	}

	if ui.err != nil {
		output += renderError(ui.err)
	}
	return output
}

func renderError(err *errors.Error) string {
	output := ""
	es := lipgloss.NewStyle().
		Foreground(lipgloss.Color("7")).
		Background(lipgloss.Color("1")).
		Bold(true).
		PaddingLeft(1).
		PaddingRight(1).
		Render("Error")
	output += fmt.Sprintf("\n%s %s\n", es, err)
	for i, frame := range err.StackFrames() {
		output += fmt.Sprintf("%3d: %s:%d\n", i+1, frame.File, frame.LineNumber)
	}
	return output
}
