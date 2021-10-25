package storagemarket

func (p *provider) loop() {
	defer p.wg.Done()

	for {
		select {

		case <-p.ctx.Done():
			return
		}
	}
}
