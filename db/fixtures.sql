INSERT INTO Deals (ID, CreatedAt, PieceCID, PieceSize, Address, Client, Provider, Label, StartEpoch, EndEpoch, StoragePricePerEpoch, ProviderCollateral, ClientCollateral, State)
VALUES
('13214', datetime('now', '-2 minute'), 'bafyreibnclzfntcs2iy7yhn3fvawv2lka4k7o5ac4n2jholpcqkvqoi4yu', 34359738368, '12D3KooWNHFrhjRPafQrNK7h9zn5fAzMKDPszchvkVuEFM4EVXTW', 'f01312', 't2000', 'f15s2bjbuj4d72heh5us2xghqrpam7z5cxqp7si6a', 519096, 519318, 4182000, 3210000, 0, 'Transferring'),
('24322', datetime('now', '-4 minute'), 'bafyreifzjh47bfbptr3wc2q2idjthnbfucud2dmv46rhl74fulrcnbhj54', 34359738368, '12D3KooWQrZnhRaBecSZYKR815zH65xkg1TvfyPWUGYwehHvVEKB', 'f00213', 't2000', 'bafyreiby3346thawnu7bwytjmwxr3obchlnuzt3xhbktbz6rdy2yxlqbhy', 512312, 514835, 4137000, 430000, 0, 'Transferring'),
('41232', datetime('now', '-20 minute'), 'bafyreibnclzfntcs2iy7yhn3fvawv2lka4k7o5ac4n2jholpcqkvqoi2yu', 34359738368, '12D3KooWQrZnhRaBecSZYKR815zH65xkg1TvfyPWUGYwehHvVEKB', 'f04429', 't2000', 'bafyreiby3346thawnu7bwytjmwxr3obchlnuzt3xhbktbz6rdy2yxlqbhy', 512312, 514835, 4137000, 430000, 0, 'Publishing'),
('41234', datetime('now', '-2 hour'), 'bafyreibnclzfntcs2iy7yhn3fvawv2lka4k7o5ac4n2jholpcqkvqoi5yu', 34359738368, '12D3KooWQrZnhRaBecSZYKR815zH65xkg1TvfyPWUGYwehHvVEKB', 'f00213', 't2000', 'bafyreiby3346thawnu7bwytjmwxr3obchlnuzt3xhbktbz6rdy2yxlqbhy', 512312, 514835, 4137000, 430000, 0, 'Pre-committing'),
('42511', datetime('now', '-4 hour'), 'bafyreibnclzfntcs2iy7yhn3fvawv2lka4k7o5ac4n2jholpcqkvqoi3yu', 34359738368, '12D3KooWQrZnhRaBecSZYKR815zH65xkg1TvfyPWUGYwehHvVEKB', 'f04429', 't2000', 'bafyreiby3346thawnu7bwytjmwxr3obchlnuzt3xhbktbz6rdy2yxlqbhy', 512312, 514835, 4137000, 430000, 0, 'Error'),
('42591', datetime('now', '-5 hour'), 'bafyreibnclzfntcs2iy7yhn3fvawv2lka4k7o5ac4n2jholpcqkvqoi3yu', 34359738368, '12D3KooWQrZnhRaBecSZYKR815zH65xkg1TvfyPWUGYwehHvVEKB', 'f01312', 't2000', 'bafyreiby3346thawnu7bwytjmwxr3obchlnuzt3xhbktbz6rdy2yxlqbhy', 512312, 514835, 4137000, 430000, 0, 'Error')
;

INSERT INTO DealLogs (DealID, CreatedAt, LogText)
VALUES
('13214', strftime('%Y-%m-%d %H:%M:%f', 'now', '-2 minute'), 'Propose Deal'),
('13214', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 minute', '+0.234 second'), 'Accepted'),
('13214', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 minute', '+1.431 second'), 'Start Data Transfer'),

('24322', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 minute'), 'Propose Deal'),
('24322', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 minute', '+0.742 second'), 'Accepted'),
('24322', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 minute', '+1.624 second'), 'Start Data Transfer'),

('41232', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-20 minute'), 'Propose Deal'),
('41232', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-20 minute', '+0.422 second'), 'Accepted'),
('41232', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-20 minute', '+0.834 second'), 'Start Data Transfer'),
('41232', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-20 minute', '+81.234 second'), 'Data Transfer Complete'),
('41232', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-20 minute', '+81.423 second'), 'Publishing'),

('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+0.163 second'), 'Propose Deal'),
('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+0.422 second'), 'Accepted'),
('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+1.153 second'), 'Start Data Transfer'),
('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+240.123 second'), 'Data Transfer Complete'),
('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+241.622 second'), 'Publishing'),
('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+820.145 second'), 'Deal Published'),
('41234', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-2 hour', '+12220.123 second'), 'Deal Pre-committed'),

('42511', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 hour', '+0.832 second'), 'Propose Deal'),
('42511', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 hour', '+1.163 second'), 'Accepted'),
('42511', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 hour', '+1.273 second'), 'Start Data Transfer'),
('42511', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-4 hour', '+53.933 second'), 'Error - Connection Lost'),

('42591', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-5 hour', '+0.729 second'), 'Propose Deal'),
('42591', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-5 hour', '+0.825 second'), 'Accepted'),
('42591', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-5 hour', '+0.923 second'), 'Start Data Transfer'),
('42591', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-5 hour', '+421.832 second'), 'Data Transfer Complete'),
('42591', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-5 hour', '+422.394 second'), 'Publishing'),
('42591', strftime('%Y-%m-%d %H:%M:%f', 'now',  '-5 hour', '+423.736 second'), 'Error - Not Enough Funds')
;