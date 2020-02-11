;; a tremor major mode, tremor-mode

;;;###autoload
;;(require 'smie)

;; Font-locking definitions and helpers
(defconst tremor-mode-keywords
  '("match" "end" "let" "when" "case" "of" "merge" "patch" "erase" "emit" "drop" "once" "default"  "or" "and" "not" "for" "null" "present" "absent" "event"))

(defconst tremor-special-types
  '("false" "null" "true"))

(defvar tremor-mode-indent-offset 2
  "*Indentation offset for `tremor-mode'.")

(defvar tremor-indent-basic 2
  "*Indentation offset for `tremor-mode'.")


(defconst tremor-re-ident "[[:word:][:multibyte:]_][[:word:][:multibyte:]_[:digit:]]*")
(defconst tremor-re-lc-ident "$?[[:lower:][:multibyte:]_][[:word:][:multibyte:]_[:digit:]]*")

(defun tremor-re-word (inner) (concat "\\<" inner "\\>"))
(defun tremor-re-grab (inner) (concat "\\(" inner "\\)"))
(defun tremor-re-shy (inner) (concat "\\(?:" inner "\\)"))
(defconst tremor-re-generic
  (concat "<[[:space:]]*'" tremor-re-ident "[[:space:]]*>"))
(defun tremor-re-item-def (itype)
  (concat (tremor-re-word itype)
	        "[[:space:]]+" (tremor-re-grab tremor-re-ident)))


(setq tremor-mode-syntax-table
      (let ( (table (make-syntax-table)))
        ;; Operators
        (dolist (i '(?+ ?* ?/ ?% ?& ?| ?^ ?! ?< ?~ ?@ ?.))
          (modify-syntax-entry i "." table))
        ;; Strings
        (modify-syntax-entry ?\" "\"" table)
        (modify-syntax-entry ?` "\"" table)
        (modify-syntax-entry ?| "\"" table)
        (modify-syntax-entry ?\\ "\\" table)
        ;;
        ;; Comments
        (modify-syntax-entry ?# "<" table)
        (modify-syntax-entry ?\n ">" table)
   table))


(defun tremor-path-font-lock-matcher (re-ident)
  "Match occurrences of RE-IDENT followed by a double-colo-n.
Examples include to match names like \"foo::\" or \"Foo::\".
Does not match type annotations of the form \"foo::<\"."
  `(lambda (limit)
     (catch 'tremor-path-font-lock-matcher
       (while t
         (let* ((symbol-then-colons (rx-to-string '(seq (group (regexp ,re-ident)) "::")))
                (match (re-search-forward symbol-then-colons limit t)))
           (cond
            ;; If we didn't find a match, there are no more occurrences
            ;; of foo::, so return.
            ((null match) (throw 'tremor-path-font-lock-matcher nil))
            ;; If this isn't a type annotation foo::<, we've found a
            ;; match, so a return it!
            ((not (looking-at (rx (0+ space) "<")))
	           (throw 'tremore-path-font-lock-matcher match))))))))


(defvar tremor-mode-font-lock-keywords
  (append
   `(
     ;; Keywords proper
     (,(regexp-opt tremor-mode-keywords 'symbols) . font-lock-keyword-face)

     ;; Special types
     (,(regexp-opt tremor-special-types 'symbols) . font-lock-type-face)

     ;; Field names like `foo:`, highlight excluding the :
     (,(concat (tremor-re-grab tremor-re-ident) ":[^:]") 1 font-lock-variable-name-face)

     ;; Module names like `foo::`, highlight excluding the ::
     (,(tremor-path-font-lock-matcher tremor-re-lc-ident) 1 font-lock-constant-face)

     )
   ))

(defun tremor-mode-syntactic-face-function (state)
  "Return face which distinguishes doc and normal comments in the given syntax STATE."
  (if (nth 3 state) 'font-lock-string-face
    (save-excursion
      (goto-char (nth 8 state))
      (if (looking-at "#[^#!]")
          'font-lock-comment-face
        'font-lock-doc-face
        ))))

(defun tremor--syntax-propertize-raw-string (end)
  "A helper for tremor-syntax-propertize.
If point is already in a raw string, this will apply the
appropriate string syntax to the character up to the end of the
raw string, or to END, whichever comes first."
  (let ((str-start (nth 8 (syntax-ppss))))
    (when str-start
      (when (save-excursion
	            (goto-char str-start)
	            (looking-at "r\\(#*\\)\\(\"\\)"))
	      ;; In a raw string, so try to find the end.
	      (let ((hashes (match-string 1)))
	        ;; Match \ characters at the end of the string to suppress
	        ;; their normal character-quote syntax.
	        (when (re-search-forward (concat "\\(\\\\*\\)\\(\"" hashes "\\)") end t)
	          (put-text-property (match-beginning 1) (match-end 1)
			                         'syntax-table (string-to-syntax "_"))
	          (put-text-property (1- (match-end 2)) (match-end 2)
			                         'syntax-table (string-to-syntax "|"))
	          (goto-char (match-end 0))))))))

(defun tremor-ordinary-lt-gt-p ()
  "Test whether the `<' or `>' at point is an ordinary operator of some kind.
This returns t if the `<' or `>' is an ordinary operator (like
less-than) or part of one (like `->'); and nil if the character
should be considered a paired angle bracket."
  nil)


(eval-and-compile
  (defconst tremor--char-literal-rx
    (rx (seq
	       (group "'")
	       (or
	        (seq
	         "\\"
	         (or
	          (: "u{" (** 1 6 xdigit) "}")
	          (: "x" (= 2 xdigit))
	          (any "'nrt0\"\\")))
	        (not (any "'\\"))
	        )
	       (group "'")))
    "A regular expression matching a character literal."))

(defun tremor-syntax-propertize (start end)
  "A `syntax-propertize-function' to apply properties from START to END."
  (goto-char start)
  (tremor--syntax-propertize-raw-string end)
  (funcall
   (syntax-propertize-rules
    ("[<>]"
     (0 (ignore
	       (when (save-match-data
		             (save-excursion
		               (goto-char (match-beginning 0))
		               (tremor-ordinary-lt-gt-p)))
	         (put-text-property (match-beginning 0) (match-end 0)
			                        'syntax-table (string-to-syntax "."))
	         (goto-char (match-end 0)))))))
   (point) end))



(defvar tremor-keywords-regexp
  (regexp-opt '("+" "*" "," ";" ">" ">=" "<" "<=" ":=" "=" "++")))

;; (defvar tremor-smie-grammar
;;   (smie-prec2->grammar
;;    (smie-bnf->prec2
;;     '((id)
;;       (inst
;;        ("let" id ":=" exp)
;;        ("erase" id)
;;        ("match" exp "of" cases "end")
;;        ("{" kvs "}")
;;        )
;;       (insts (inst ";" inst))

;;       ;;(kvs (id ":" exp) (kvs "," kvs))
;;       (cases (cases ";" cases)
;;              ("case" caselabel
;;               "=>" exp)
;;              ("case" caselabel "when" exp
;;               "=>" exp))
;;       (exp (inst))
;;       (exp (exp "+" exp)
;;            (exp "-" exp)
;;            (exp "*" exp)
;;            (exp "/" exp)
;;            (id)
;;            (inst)
;;            ("(" exps ")")))
;;     '((assoc ":"))
;;     '((assoc ","))
;;     '((assoc ";"))4
;;     '((assoc "_" "->"))
;;     '((assoc "+") (assoc "-") (assoc "*") (assoc "/")))))


;; (defvar tremor-smie-grammar
;;   (smie-prec2->grammar
;;    (smie-bnf->prec2
;;     '((id)
;;       (stmt
;;        ("let" id ":=" expr)
;;        ("erase" id)
;;        ("extend" expr "match" expr "->" cases "end")
;;        (expr))
;;       (stmts (stmt ";" stmt))
;;       (expr
;;        ("match" expr "->" cases "end")
;;        ("[" items "]")
;;        ("{" oitems "}")
;;        (id))
;;       (aitems (aitems "," aitems), expr)
;;       (oitems (oitems "," oitems), (expr ":" expr))
;;       ;;("case" caselabel "when" exp "=>" stmts)
;;       (cases (cases ";" cases)
;;              ("case" caselabel "=>" stmts)))
;;     '((assoc "extend" "match"))
;;     '((assoc  "=>"))
;;     '((assoc ";"))
;;     '((assoc ","))
;;     '((assoc ":"))
;;     ;;'((assoc ":"))
;;     ;;'((assoc "_" "->"))
;;     '((assoc "+") (assoc "-") (assoc "*") (assoc "/")))))



;; (defvar tremor-smie-grammar
;;   (smie-prec2->grammar
;;    (smie-bnf->prec2
;;     '((id)
;;       (stmt
;;        ("let" id "=" expr)
;;        (expr))
;;       (stmts (stmt ";" stmt))
;;       (case-stmts (stmt "," stmt))
;;       (expr
;;        ("match" expr "of" cases "default" effectors "end")
;;        ("patch" expr "of" patches "end")
;;        ("for" expr "of" cases "end")
;;        ("merge" expr "of" expr "end")
;;        ("[" items "]")
;;        ("{" oitems "}")
;;        (id))
;;       (aitems (aitems "," aitems), expr)
;;       (oitems (oitems "," oitems), (expr ":" expr))
;;       (cases (cases " " cases)
;;              ("case" caselabel effectors))
;;       (effectors ("=>" case-stmts)))
;;     '((assoc "=>"))
;;     '((assoc ";"))
;;     '((assoc ","))
;;     '((assoc ":"))
;;     '((assoc "+") (assoc "-") (assoc "*") (assoc "/")))))

;; (defun tremor-smie-rules (kind token)
;;   (pcase (cons kind token)
;;     (`(:elem . basic) tremor-indent-basic)
;;     (`(,_ . ",") (smie-rule-separator kind))
;;     ;; (`(,_ . ";") (smie-rule-separator kind))
;;     (`(,_ . ":") (smie-rule-separator kind))
;;     ;; (`(,_ . "when") (smie-rule-separator kind))
;;     ;; (`(:after . ":") tremor-indent-basic)
;;     (`(:before . "=") tremor-indent-basic)
;;     ;;(`(:before . "case") tremor-indent-basic)
;;     ;;(`(:before . "default") tremor-indent-basic)
;;     ;; (`(:before . "=>") tremor-indent-basic)
;;     ;; (`(:before . "when") tremor-indent-basic)
;;     ;; (`(:list-intro . "") tremor-indent-basic)
;;     (`(:before . ,(or `"[" `"(" `"{"))
;;      (if (smie-rule-hanging-p) (smie-rule-parent)))
;;     ;; (`(:before . "extend") (smie-rule-parent))
;;     ;; (`(:before . "match") (smie-rule-parent))
;;     ;; (`(:before . "->") tremor-indent-basic)
;;     (`(:after . "of") tremor-indent-basic)
;;     ;; (`(:before . "with") tremor-indent-basic)
;;     ;; (`(:before . "end") (smie-rule-parent))
;;     ))

;;;###autoload
(define-derived-mode tremor-mode prog-mode "Tremor"
  "Major mode for Tremor code.
\\{tremor-mode-map}"
  :group 'tremor-mode
  :syntax-table tremor-mode-syntax-table

  ;; Syntax.
  (setq-local syntax-propertize-function #'tremor-syntax-propertize)

  ;; Indentation
  ;;(setq-local indent-line-function 'tremor-mode-indent-line)
  ;;(smie-setup nil #'tremor-smie-rules)
  ;;(setq-local smie-indent-basic tremor-indent-basic)
  ;;(setq-local smie-grammar tremor-smie-grammar)

  ;; Fonts
  (setq-local font-lock-defaults '(tremor-mode-font-lock-keywords
                                   nil nil nil nil
                                   (font-lock-syntactic-face-function . tremor-mode-syntactic-face-function)))

  (setq-local comment-use-syntax t)
  (setq-local comment-start "#")
  (setq-local comment-end "")
  (setq-local indent-tabs-mode nil)

  ;; ;; Misc
  ;; (setq-local comment-start "// ")
  ;; (setq-local comment-end   "")
  ;; (setq-local indent-tabs-mode nil)
  ;; (setq-local open-paren-in-column-0-is-defun-start nil)

  ;; ;; Auto indent on }
  ;; (setq-local
  ;;  electric-indent-chars (cons ?} (and (boundp 'electric-indent-chars)
  ;;                                      electric-indent-chars)))

  ;; ;; Allow paragraph fills for comments
  ;; ;; (setq-local comment-start-skip "\\(?://[/!]*\\|/\\*[*!]?\\)[[:space:]]*")
  ;; (setq-local paragraph-start
  ;;      (concat "[[:space:]]*\\(?:" comment-start-skip "\\|\\*/?[[:space:]]*\\|\\)$"))
  ;; (setq-local paragraph-separate paragraph-start)
  ;; (setq-local normal-auto-fill-function 'tremor-do-auto-fill)
  ;; (setq-local fill-paragraph-function 'tremor-fill-paragraph)
  ;; (setq-local fill-forward-paragraph-function 'tremor-fill-forward-paragraph)
  ;; (setq-local adaptive-fill-function 'tremor-find-fill-prefix)
  ;; (setq-local adaptive-fill-first-line-regexp "")
  ;; (setq-local comment-multi-line t)
  ;; (setq-local comment-line-break-function 'tremor-comment-indent-new-line)
  ;; (setq-local imenu-generic-expr ession tremor-imenu-generic-expression)
  ;; (setq-local imenu-syntax-alist '((?! . "w"))) ; For macro_rules!
  ;; (setq-local beginning-of-defun-function 'tremor-beginning-of-defun)
  ;; (setq-local end-of-defun-function 'tremor-end-of-defun)
  ;; (setq-local parse-sexp-lookup-properties t)
  ;; (setq-local electric-pair-inhibit-predicate 'tremor-electric-pair-inhibit-predicate-wrap)

  ;; (add-hook 'before-save-hook 'tremor-before-save-hook nil t)
  ;; (add-hook 'after-save-hook 'tremor-after-save-hook nil t)

  ;; (setq-local tremor-buffer-project nil)

  ;; (when tremor-always-locate-project-on-open
  ;;   (tremor-update-buffer-project)))
)

(add-to-list 'auto-mode-alist '("\\.tremor\\'" . tremor-mode))

(provide 'tremor-mode)
