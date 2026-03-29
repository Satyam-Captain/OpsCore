/**
 * Guided service inputs: fetch field help JSON and render under each field.
 * No framework; keep logic minimal.
 */
(function () {
  function escapeHtml(s) {
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function renderUniqueValues(container, values) {
    if (!values || !values.length) {
      container.innerHTML =
        '<span class="page-description">No sample values in mock DB for this column.</span>';
      return;
    }
    var ul = document.createElement("ul");
    ul.className = "help-values-list";
    values.forEach(function (v) {
      var li = document.createElement("li");
      li.textContent = v === null || v === undefined ? "" : String(v);
      ul.appendChild(li);
    });
    container.innerHTML = "";
    container.appendChild(ul);
  }

  function renderLookupRows(container, rows) {
    if (!rows || !rows.length) {
      container.innerHTML =
        '<span class="page-description">No rows in mock DB for this lookup.</span>';
      return;
    }
    var table = document.createElement("table");
    table.className = "help-lookup-table";
    var thead = document.createElement("thead");
    var trh = document.createElement("tr");
    var keys = Object.keys(rows[0]);
    keys.forEach(function (k) {
      var th = document.createElement("th");
      th.textContent = k;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);
    var tbody = document.createElement("tbody");
    rows.forEach(function (row) {
      var tr = document.createElement("tr");
      keys.forEach(function (k) {
        var td = document.createElement("td");
        var val = row[k];
        td.textContent = val === null || val === undefined ? "" : String(val);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    container.innerHTML = "";
    container.appendChild(table);
  }

  function bindHelpButtons() {
    document.querySelectorAll(".services-help-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var url = btn.getAttribute("data-help-url");
        var tid = btn.getAttribute("data-help-target");
        var out = tid ? document.getElementById(tid) : null;
        if (!url || !out) return;
        out.innerHTML =
          '<span class="page-description">Loading…</span>';
        fetch(url, { credentials: "same-origin" })
          .then(function (r) {
            return r.json().then(function (data) {
              return { ok: r.ok, status: r.status, data: data };
            });
          })
          .then(function (res) {
            if (!res.ok) {
              out.innerHTML =
                '<span class="alert alert-error" style="display:inline-block;margin-top:0.35rem;">' +
                escapeHtml(res.data.error || "Help request failed") +
                "</span>";
              return;
            }
            var d = res.data;
            if (d.mode === "unique_values") {
              renderUniqueValues(out, d.values);
            } else if (d.mode === "lookup_rows") {
              renderLookupRows(out, d.rows);
            } else {
              out.innerHTML =
                '<span class="page-description">Unexpected help response.</span>';
            }
          })
          .catch(function () {
            out.innerHTML =
              '<span class="alert alert-error" style="display:inline-block;margin-top:0.35rem;">Network error</span>';
          });
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindHelpButtons);
  } else {
    bindHelpButtons();
  }
})();
