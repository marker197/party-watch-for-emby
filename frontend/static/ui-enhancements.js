/**
 * Enhanced UI/UX Module
 * 
 * Features:
 * - Dark/Light mode toggle (persistent)
 * - Mobile-responsive design
 * - Lazy loading for charts
 * - Smooth animations
 * - Better accessibility
 * - Performance optimizations
 */

// Theme Management
class ThemeManager {
    constructor() {
        this.darkModeKey = 'emby-trakt-dark-mode';
        this.init();
    }

    init() {
        const isDark = this.getSavedTheme() || this.getSystemPreference();
        this.setTheme(isDark);
        this.setupToggle();
    }

    getSavedTheme() {
        const saved = localStorage.getItem(this.darkModeKey);
        return saved ? saved === 'true' : null;
    }

    getSystemPreference() {
        return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    }

    setTheme(isDark) {
        const root = document.documentElement;
        if (isDark) {
            root.setAttribute('data-theme', 'dark');
            document.body.classList.add('dark-mode');
        } else {
            root.setAttribute('data-theme', 'light');
            document.body.classList.remove('dark-mode');
        }
        localStorage.setItem(this.darkModeKey, isDark.toString());
    }

    toggle() {
        const isDark = document.body.classList.contains('dark-mode');
        this.setTheme(!isDark);
        this.updateToggleButton();
    }

    setupToggle() {
        const toggleBtn = document.getElementById('themeToggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', () => this.toggle());
            this.updateToggleButton();
        }
    }

    updateToggleButton() {
        const toggleBtn = document.getElementById('themeToggle');
        if (toggleBtn) {
            const isDark = document.body.classList.contains('dark-mode');
            toggleBtn.textContent = isDark ? '☀️ Light Mode' : '🌙 Dark Mode';
            toggleBtn.setAttribute('aria-label', `Switch to ${isDark ? 'light' : 'dark'} mode`);
        }
    }
}

// Lazy Loading for Charts and Heavy Components
class LazyLoader {
    constructor(options = {}) {
        this.options = {
            rootMargin: '50px',
            threshold: 0.01,
            ...options
        };
        this.observers = new Map();
        this.init();
    }

    init() {
        const observer = new IntersectionObserver(
            (entries) => this.handleIntersection(entries),
            this.options
        );

        document.querySelectorAll('[data-lazy]').forEach(el => {
            observer.observe(el);
            this.observers.set(el, observer);
        });
    }

    handleIntersection(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                this.loadElement(entry.target);
            }
        });
    }

    loadElement(el) {
        const loader = el.getAttribute('data-lazy');
        const observer = this.observers.get(el);
        if (observer) observer.unobserve(el);

        if (loader === 'chart') {
            this.loadChart(el);
        } else if (loader === 'image') {
            this.loadImage(el);
        } else if (loader === 'content') {
            this.loadContent(el);
        }

        el.classList.add('loaded');
    }

    loadChart(el) {
        el.classList.add('chart-loading');
        el.innerHTML = '<div class="spinner"></div>';
        
        // Delay to allow paint
        requestAnimationFrame(() => {
            const event = new CustomEvent('render-chart', { detail: { element: el } });
            document.dispatchEvent(event);
            el.classList.remove('chart-loading');
        });
    }

    loadImage(el) {
        const src = el.getAttribute('data-src');
        if (src) {
            el.src = src;
            el.removeAttribute('data-src');
        }
    }

    loadContent(el) {
        const url = el.getAttribute('data-url');
        if (url) {
            fetch(url)
                .then(r => r.text())
                .then(html => { el.innerHTML = html; })
                .catch(e => { el.innerHTML = '<p>Failed to load</p>'; });
        }
    }
}

// Mobile Menu Toggle
class MobileMenu {
    constructor() {
        this.menuBtn = document.getElementById('mobileMenuBtn');
        this.nav = document.querySelector('nav');
        if (this.menuBtn) {
            this.init();
        }
    }

    init() {
        this.menuBtn.addEventListener('click', () => this.toggle());
        document.addEventListener('click', (e) => {
            if (!e.target.closest('nav') && !e.target.closest('#mobileMenuBtn')) {
                this.close();
            }
        });
    }

    toggle() {
        this.nav?.classList.toggle('mobile-open');
        this.menuBtn.setAttribute('aria-expanded', 
            this.nav?.classList.contains('mobile-open') ? 'true' : 'false');
    }

    close() {
        this.nav?.classList.remove('mobile-open');
        this.menuBtn?.setAttribute('aria-expanded', 'false');
    }
}

// Performance Monitor
class PerformanceMonitor {
    static measure(name, fn) {
        performance.mark(`${name}-start`);
        const result = fn();
        performance.mark(`${name}-end`);
        performance.measure(name, `${name}-start`, `${name}-end`);
        
        const measure = performance.getEntriesByName(name)[0];
        console.debug(`${name}: ${measure.duration.toFixed(2)}ms`);
        return result;
    }

    static async measureAsync(name, fn) {
        performance.mark(`${name}-start`);
        const result = await fn();
        performance.mark(`${name}-end`);
        performance.measure(name, `${name}-start`, `${name}-end`);
        
        const measure = performance.getEntriesByName(name)[0];
        console.debug(`${name}: ${measure.duration.toFixed(2)}ms`);
        return result;
    }

    static getReport() {
        const measures = performance.getEntriesByType('measure');
        return measures.map(m => ({
            name: m.name,
            duration: m.duration.toFixed(2)
        }));
    }
}

// Smooth Scroll and Animations
class UIAnimations {
    static init() {
        this.setupScrollAnimations();
        this.setupTransitions();
    }

    static setupScrollAnimations() {
        const observer = new IntersectionObserver(
            (entries) => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        entry.target.classList.add('in-view');
                    }
                });
            },
            { threshold: 0.1 }
        );

        document.querySelectorAll('.card, .chart-container').forEach(el => {
            el.classList.add('fade-in');
            observer.observe(el);
        });
    }

    static setupTransitions() {
        // Disable animations during page load
        document.documentElement.style.scrollBehavior = 'auto';
        
        // Enable smooth scroll after load
        window.addEventListener('load', () => {
            document.documentElement.style.scrollBehavior = 'smooth';
        });
    }

    static fadeOut(el, duration = 300) {
        return new Promise((resolve) => {
            el.style.animation = `fadeOut ${duration}ms ease-out forwards`;
            setTimeout(resolve, duration);
        });
    }

    static fadeIn(el, duration = 300) {
        el.style.opacity = '0';
        el.style.animation = `fadeIn ${duration}ms ease-in forwards`;
    }
}

// Form Validation and Better UX
class FormHelper {
    static setupValidation() {
        document.querySelectorAll('form').forEach(form => {
            form.addEventListener('submit', (e) => {
                if (!form.checkValidity()) {
                    e.preventDefault();
                    this.showValidationErrors(form);
                }
            });

            form.querySelectorAll('input, select, textarea').forEach(field => {
                field.addEventListener('blur', () => {
                    this.validateField(field);
                });
            });
        });
    }

    static validateField(field) {
        const isValid = field.checkValidity();
        const container = field.closest('.form-group');
        
        if (!isValid) {
            container?.classList.add('error');
            const error = container?.querySelector('.error-message');
            if (error) {
                error.textContent = field.validationMessage;
            }
        } else {
            container?.classList.remove('error');
        }
    }

    static showValidationErrors(form) {
        const fields = form.querySelectorAll('input, select, textarea');
        const firstInvalid = Array.from(fields).find(f => !f.checkValidity());
        
        if (firstInvalid) {
            firstInvalid.focus();
            firstInvalid.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

// Search and Filter with Debouncing
class SearchFilter {
    constructor(inputSelector, listSelector, options = {}) {
        this.input = document.querySelector(inputSelector);
        this.list = document.querySelector(listSelector);
        this.options = {
            debounceDelay: 300,
            caseSensitive: false,
            ...options
        };
        
        if (this.input && this.list) {
            this.init();
        }
    }

    init() {
        this.debounceTimer = null;
        this.input.addEventListener('input', (e) => {
            clearTimeout(this.debounceTimer);
            this.debounceTimer = setTimeout(() => {
                this.filter(e.target.value);
            }, this.options.debounceDelay);
        });
    }

    filter(term) {
        const query = this.options.caseSensitive ? term : term.toLowerCase();
        let visibleCount = 0;

        this.list.querySelectorAll('[data-searchable]').forEach(item => {
            const text = this.options.caseSensitive 
                ? item.textContent 
                : item.textContent.toLowerCase();
            
            const isMatch = text.includes(query);
            item.style.display = isMatch ? '' : 'none';
            if (isMatch) visibleCount++;
        });

        // Show "no results" message
        const noResults = this.list.querySelector('[data-no-results]');
        if (noResults) {
            noResults.style.display = visibleCount === 0 ? '' : 'none';
        }
    }
}

// Toast Notifications
class Toast {
    static show(message, type = 'info', duration = 3000) {
        const container = this.ensureContainer();
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.setAttribute('role', 'status');
        toast.setAttribute('aria-live', 'polite');
        toast.textContent = message;
        
        container.appendChild(toast);
        
        // Trigger animation
        requestAnimationFrame(() => {
            toast.classList.add('show');
        });
        
        if (duration) {
            setTimeout(() => {
                toast.classList.remove('show');
                setTimeout(() => toast.remove(), 300);
            }, duration);
        }

        return toast;
    }

    static ensureContainer() {
        let container = document.getElementById('toastContainer');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toastContainer';
            container.setAttribute('role', 'region');
            container.setAttribute('aria-label', 'Notifications');
            document.body.appendChild(container);
        }
        return container;
    }

    static success(message, duration) {
        return this.show(message, 'success', duration);
    }

    static error(message, duration) {
        return this.show(message, 'error', duration);
    }

    static warning(message, duration) {
        return this.show(message, 'warning', duration);
    }

    static info(message, duration) {
        return this.show(message, 'info', duration);
    }
}

// Pagination Helper
class Paginator {
    constructor(itemsPerPage = 20) {
        this.itemsPerPage = itemsPerPage;
        this.currentPage = 1;
    }

    paginate(items) {
        const start = (this.currentPage - 1) * this.itemsPerPage;
        return items.slice(start, start + this.itemsPerPage);
    }

    getTotalPages(totalItems) {
        return Math.ceil(totalItems / this.itemsPerPage);
    }

    render(items, containerSelector) {
        const container = document.querySelector(containerSelector);
        if (!container) return;

        const total = items.length;
        const pageItems = this.paginate(items);
        const totalPages = this.getTotalPages(total);

        container.innerHTML = '';
        pageItems.forEach(item => {
            const el = document.createElement('div');
            el.innerHTML = item.html || item;
            container.appendChild(el.firstElementChild);
        });

        // Render pagination controls
        this.renderControls(totalPages, containerSelector);
    }

    renderControls(totalPages, containerSelector) {
        const container = document.querySelector(containerSelector);
        const nav = document.createElement('nav');
        nav.className = 'pagination';
        nav.setAttribute('aria-label', 'Pagination');

        // Previous button
        const prev = document.createElement('button');
        prev.textContent = '← Previous';
        prev.disabled = this.currentPage === 1;
        prev.onclick = () => this.goToPage(this.currentPage - 1);
        nav.appendChild(prev);

        // Page indicator
        const indicator = document.createElement('span');
        indicator.textContent = `Page ${this.currentPage} of ${totalPages}`;
        nav.appendChild(indicator);

        // Next button
        const next = document.createElement('button');
        next.textContent = 'Next →';
        next.disabled = this.currentPage === totalPages;
        next.onclick = () => this.goToPage(this.currentPage + 1);
        nav.appendChild(next);

        container.parentElement.appendChild(nav);
    }

    goToPage(page) {
        this.currentPage = Math.max(1, Math.min(page, this.getTotalPages(/* items count needed */)));
    }
}

// Initialize all on page load
document.addEventListener('DOMContentLoaded', () => {
    // Initialize theme manager
    new ThemeManager();
    
    // Initialize lazy loading
    new LazyLoader();
    
    // Initialize mobile menu
    new MobileMenu();
    
    // Setup animations
    UIAnimations.init();
    
    // Setup form validation
    FormHelper.setupValidation();
    
    console.log('UI enhancements initialized');
});

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        ThemeManager,
        LazyLoader,
        UIAnimations,
        FormHelper,
        SearchFilter,
        Toast,
        PerformanceMonitor,
        Paginator,
        MobileMenu
    };
}
