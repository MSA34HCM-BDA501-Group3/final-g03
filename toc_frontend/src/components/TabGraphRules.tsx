import React, { useEffect, useState } from 'react';
import {
  CardB2Row,
  CoCongestionRuleItem,
  GraphRulesResponse,
  LaggedRuleItem,
  PageRankTopItem,
} from '../types';

interface Props {
  onSelectSegmentOnMap: (segId: number) => void;
}

export const TabGraphRules: React.FC<Props> = ({ onSelectSegmentOnMap }) => {
  const [data, setData] = useState<GraphRulesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/graph-rules')
      .then((r) => {
        if (!r.ok) throw new Error(`Lỗi máy chủ (${r.status}) khi tải dữ liệu đồ thị & luật.`);
        return r.json() as Promise<GraphRulesResponse>;
      })
      .then((d) => {
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Không thể tải dữ liệu đồ thị & luật đồng kẹt.');
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) {
    return (
      <div className="card" role="alert" style={{ color: '#991b1b', background: '#fef2f2', borderColor: '#fecaca' }}>
        ⚠️ {error}
      </div>
    );
  }
  if (!data) return <div className="card">Đang tải dữ liệu Đồ thị Line-Graph PageRank &amp; Luật SON...</div>;

  const b2Part1 = (data.card_b2_falsification || []).filter((r: CardB2Row) =>
    String(r.test_category || '').startsWith('B2_PART1')
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 15 }}>
      <div className="card">
        <div className="card-header">
          <div>
            <h2 className="card-title">
              Kiểm Nghiệm Phản Biện Card B2: Vì Sao PageRank Cao Không Đồng Nghĩa Đang Kẹt Xe?
            </h2>
            <p className="card-subtitle">
              Đối chứng trực tiếp giữa cấu trúc hình học đồ thị (giao lộ thực sau khi lọc điểm uốn cong) và vận tốc GPS thực nghiệm
            </p>
          </div>
          <span className="badge badge-blue">UC-3 &amp; Card B2 Verified</span>
        </div>
        <div className="table-scroll" style={{ maxHeight: 230 }}>
          <table className="toc-table">
            <thead>
              <tr>
                <th>Nhóm Đối Chứng (Card B2)</th>
                <th>Đoạn &amp; Tuyến đường</th>
                <th>PageRank</th>
                <th>Số khung GPS</th>
                <th>Vận tốc TB / Vmax</th>
                <th>Thực tế &amp; Kết luận Giải nghĩa</th>
              </tr>
            </thead>
            <tbody>
              {b2Part1.map((r: CardB2Row, i: number) => {
                const isFree = String(r.test_category).includes('UNCONGESTED');
                return (
                  <tr key={i}>
                    <td>
                      <span className={`badge ${isFree ? 'badge-green' : 'badge-red'}`}>
                        {isFree ? 'PageRank Cao · Thông Thoáng' : 'PageRank Cao · Điểm Nghẽn'}
                      </span>
                    </td>
                    <td>
                      <strong>{r.name_or_scope}</strong> ({r.entity_id})
                    </td>
                    <td>
                      <b>{r.pagerank_or_size}</b>
                    </td>
                    <td>{r.observed_windows} khung</td>
                    <td>
                      <b>{r.mean_velocity_kmh}</b> / {r.max_velocity_kmh} km/h
                    </td>
                    <td>{r.empirical_los_or_span}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">1. Top-{data.pagerank_top.length} Nút Cổ Chai Cấu Trúc Directed Line-Graph PageRank (UC-3)</h2>
              <p className="card-subtitle">Nhấp vào bất kỳ đoạn đường nào để xem trên bản đồ &amp; chuỗi dự báo</p>
            </div>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Tuyến đường</th>
                  <th>Mã đoạn</th>
                  <th>PageRank</th>
                  <th>Vào/Ra</th>
                  <th>Trạng thái GPS</th>
                </tr>
              </thead>
              <tbody>
                {(data.pagerank_top || []).map((r: PageRankTopItem, idx: number) => (
                  <tr
                    key={r.segment_id}
                    className="clickable"
                    onClick={() => onSelectSegmentOnMap(Number(r.segment_id))}
                  >
                    <td>{idx + 1}</td>
                    <td>
                      <strong>{r.street_name}</strong>
                    </td>
                    <td>#{r.segment_id}</td>
                    <td>
                      <b>{Number(r.pagerank_score).toFixed(3)}</b>
                    </td>
                    <td>
                      {r.in_degree} / {r.out_degree}
                    </td>
                    <td>
                      {r.is_cold_start ? (
                        <span className="badge badge-amber">Cold-Start (0 GPS)</span>
                      ) : (
                        <span className="badge badge-green">Có GPS ({r.obs_windows} khung)</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">2. Cặp Tuyến Đường Đồng Kẹt 2-Pass SON / FP-Growth (UC-4)</h2>
              <p className="card-subtitle">Đã chuẩn hóa Unicode NFC thực thể đường phố, không cắt ngọn giỏ</p>
            </div>
          </div>
          <div className="table-scroll">
            <table className="toc-table">
              <thead>
                <tr>
                  <th>Tuyến A (Đang E/F)</th>
                  <th>➔ Tuyến B (Cùng E/F)</th>
                  <th>Số khung</th>
                  <th>Confidence</th>
                  <th>Lift</th>
                </tr>
              </thead>
              <tbody>
                {(data.co_congestion_rules || []).map((r: CoCongestionRuleItem, i: number) => (
                  <tr key={i}>
                    <td>
                      <strong>{r.antecedent_street}</strong>
                    </td>
                    <td>
                      <strong>{r.consequent_street}</strong>
                    </td>
                    <td>{r.co_congested_windows ?? r.support_count} khung</td>
                    <td>{(Number(r.confidence) * 100).toFixed(1)}%</td>
                    <td>
                      <b style={{ color: '#2563eb' }}>{Number(r.lift).toFixed(2)}x</b>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="card-header">
          <div>
            <h2 className="card-title">3. Giả Thuyết Lan Truyền Kẹt Xe Trễ Sau +30 Phút (t ➔ t + 30m)</h2>
            <p className="card-subtitle">
              Minh bạch học thuật: do độ thưa cảm biến, cỡ mẫu đồng xuất hiện còn nhỏ nên được gắn nhãn Exploratory Hypothesis
            </p>
          </div>
          <span className="badge badge-amber">Exploratory Hypothesis</span>
        </div>
        <div className="table-scroll" style={{ maxHeight: 220 }}>
          <table className="toc-table">
            <thead>
              <tr>
                <th>Tuyến Kẹt Tại t</th>
                <th>➔ Tuyến Kẹt Kế Tiếp Tại t + 30p</th>
                <th>Số khung chung</th>
                <th>Confidence</th>
                <th>Lift</th>
              </tr>
            </thead>
            <tbody>
              {(data.lagged_rules || []).map((r: LaggedRuleItem, i: number) => (
                <tr key={i}>
                  <td>
                    <strong>{r.antecedent_street_t || r.source_street_t || r.antecedent_street}</strong>
                  </td>
                  <td>
                    <strong>{r.consequent_street_t30m || r.target_street_t30m || r.consequent_street}</strong>
                  </td>
                  <td>{r.lagged_transition_windows ?? r.support_count} khung</td>
                  <td>{(Number(r.confidence) * 100).toFixed(1)}%</td>
                  <td>
                    <b>{Number(r.lift).toFixed(2)}x</b>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
