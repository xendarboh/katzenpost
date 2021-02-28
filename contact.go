package main

import (
	"gioui.org/layout"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// AddContactComplete is emitted when catshadow.NewContact has been called
type AddContactComplete struct {
	nickname string
}

// AddContactPage is the page for adding a new contact
type AddContactPage struct {
	nickname *widget.Editor
	secret   *widget.Editor
	submit   *widget.Clickable
}

// Layout returns a simple centered layout prompting user for contact nickname and secret
func (p *AddContactPage) Layout(gtx layout.Context) layout.Dimensions {
	bg := Background{
		Color: th.Bg,
		Inset: layout.Inset{},
	}

	return bg.Layout(gtx, func(gtx C) D {
		return layout.Flex{Axis: layout.Vertical, Alignment: layout.End}.Layout(gtx,
			layout.Flexed(1, func(gtx C) D { return layout.Center.Layout(gtx, material.Editor(th, p.nickname, "nickname").Layout) }),
			layout.Flexed(1, func(gtx C) D { return layout.Center.Layout(gtx, material.Editor(th, p.secret, "secret").Layout) }),
			layout.Rigid(func(gtx C) D { return material.Button(th, p.submit, "MEOW").Layout(gtx) }),
		)
	})
}

// Event catches the widget submit events and calls catshadow.NewContact
func (p *AddContactPage) Event(gtx layout.Context) interface{} {
	for _, ev := range p.nickname.Events() {
		switch ev.(type) {
		case widget.SubmitEvent:
			p.secret.Focus()
		}
	}
	for _, ev := range p.secret.Events() {
		switch ev.(type) {
		case widget.SubmitEvent:
			p.submit.Click()
		}
	}
	if p.submit.Clicked() {
		if len(p.secret.Text()) < minPasswordLen {
			p.secret.SetText("")
			p.secret.Focus()
			return nil
		}
		catshadowClient.NewContact(p.nickname.Text(), []byte(p.secret.Text()))
		return AddContactComplete{nickname: p.nickname.Text()}
	}
	return nil
}

func (p *AddContactPage) Start(stop <-chan struct{}) {
}

func newAddContactPage() *AddContactPage {
	p := &AddContactPage{}
	p.nickname = &widget.Editor{SingleLine: true, Submit: true}
	p.nickname.Focus()
	p.secret = &widget.Editor{SingleLine: true, Submit: true, Mask: '*'}
	p.submit = &widget.Clickable{}
	return p
}
