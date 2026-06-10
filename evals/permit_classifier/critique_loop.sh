#!/bin/bash
# Incremental critique driver: runs critique_claude.py whenever >=50 new qwen
# labels accumulate, finishes when all 500 are critiqued. Detached via nohup.
cd /home/will/permit-api
while true; do
  nl=$(wc -l < evals/permit_classifier/qwen_labels.jsonl 2>/dev/null || echo 0)
  nc=$(wc -l < evals/permit_classifier/critique.jsonl 2>/dev/null || echo 0)
  if [ "$nl" -ge 500 ] && [ "$nc" -ge "$nl" ]; then echo "CRITIQUE_COMPLETE $nc"; break; fi
  if [ "$((nl - nc))" -ge 50 ] || { [ "$nl" -ge 500 ] && [ "$nc" -lt "$nl" ]; }; then
    python3 evals/permit_classifier/critique_claude.py 2>&1 | tail -2
  fi
  if ! pgrep -f "label_qwen.py|run_eval_labels.py" >/dev/null && [ "$nl" -lt 500 ]; then
    nl2=$(wc -l < evals/permit_classifier/qwen_labels.jsonl)
    if [ "$nl2" -lt 500 ]; then echo "LABELER_DEAD at $nl2"; break; fi
  fi
  sleep 120
done
